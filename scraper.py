# coding: utf8
from bs4 import BeautifulSoup
import datetime
from dateutil.relativedelta import relativedelta
import re
import requests_cache
import requests

from requests.exceptions import ConnectionError

import numpy as np
import pandas as pd
import pandas_datareader.data as web

from time import time

from db import DB
from utils import (
    cache_path,
    pickled_page_exists,
    pickle_response,
    load_pickled_page,
)


FUND_FAMILIES = "http://quicktake.morningstar.com/fundfamily/0C00001Z4B/all-fund-family.aspx"
MORNINGSTAR = "http://quicktake.morningstar.com"

CACHE_PATH = cache_path()
EXPIRE_AFTER = datetime.timedelta(days=7)

split_new_line = lambda x: x.split("\n")[0]
soupit = lambda x: BeautifulSoup(x, "html.parser")


class MFScraper:
    def __init__(self, db_path, ds, cache_path, cache_expire_days,
                 start_date, end_date, limit=[]):
        self.db = DB(db_path)
        self.ds=ds
        self.cache_expire_days=datetime.timedelta(days=cache_expire_days)
        self.session = requests_cache.CachedSession(cache_name=cache_path,
                                                    backend="sqlite",
                                                    expire_after=self.cache_expire_days)
        self.start_date = start_date
        self.end_date = end_date
        self.limit = limit
        self.ignore = {
            "families": [
                "TOPS",
                "Symmetry Partners",
                "AXA Equitable",
            ],
            "symbols": [
                "HYPPX",
                "JCUNX",
                "JHBC",
            ]
        }

    def scrape(self, symbol, start_date, end_date):
        try:
            response = web.DataReader(symbol, self.ds, start_date,
                                      end_date, session=self.session)
        except KeyError:
            print(
                "Could not retrieve prices for: {}, using {}"
                .format(symbol,self.ds)
            )
            return None
        self.db.log_symbol_lookup(symbol)
        return response

    def _load_fund_families_table(self):
        self._ensure_pickle(FUND_FAMILIES)
        content = load_pickled_page(FUND_FAMILIES)
        soup = soupit(content)
        return soup.find("table")

    def get_fund_families(self):
        table = self._load_fund_families_table()

        if self.limit:
            fund_families = self._find_specific_fund_families(table, self.limit)
        else:
            fund_families = self._find_all_fund_families(table)
        return fund_families

    def _find_specific_fund_families(self, table, limit):
        fund_families = dict()
        for link in table.find_all("a"):
            link_name = link.get_text()
            if link_name:
                for li in limit:
                    if re.match(li, link_name, re.IGNORECASE):
                        href = split_new_line(link["href"])
                        fund_families[link_name] = {
                            "href": href
                        }
                        break
        return fund_families

    def _find_all_fund_families(self, table):
        fund_families = dict()
        for link in table.find_all("a"):
            if link.get_text() and link.get_text() not in self.ignore["families"]:
                href = split_new_line(link["href"])
                fund_families[link.get_text()] = {
                        "href": href
                    }
        return fund_families

    def _ensure_pickle(self, url):
        if not pickled_page_exists(url):
            try:
                response = requests.get(url)
            except ConnectionError:
                return
            pickle_response(response)

    def get_fund_page(self, fund_family):
        fund_page = None
        url = MORNINGSTAR + fund_family["href"]

        self._ensure_pickle(url)
        content = load_pickled_page(url)
        soup = soupit(content)

        div = soup.find("div", {"class": "tsgroup1 noborderbottom"})
        if div:
            ul = div.find("ul")
            a_elem = ul.find("a")
            fund_page = split_new_line(a_elem["href"])

        return fund_page


    def get_all_symbols(self, fund_family):
        if fund_family["fund_page"] is None:
            return list()

        symbol_regex = 't\=([A-Z]*)"'

        url = MORNINGSTAR + fund_family["fund_page"]

        self._ensure_pickle(url)
        content = load_pickled_page(url)
        soup = soupit(content)

        table = soup.find("table")
        cells = table.find_all("td", {"class": "msNormal"})

        seen = set()
        symbols = []
        for cell in cells:
            name = cell.get_text()
            if not name:
                continue

            href = cell.find("a")
            if not href:
                continue

            symbol = re.findall(symbol_regex, str(href))
            if not symbol:
                continue
            s = symbol[0]

            if s not in seen and s not in self.ignore["symbols"]:
                symbols.append({
                    "symbol": s,
                    "name": name,
                })
                seen.add(s)
        return symbols

    def _find_last_lookup(self, symbol):
        last_symbol_lookup = self.db.last_symbol_lookup(symbol)
        if last_symbol_lookup is None:
            return (None, None)
        return last_symbol_lookup

    def add_columns_to_df(self, df, d={}):
        for k, v in d.items():
            df[k] = v
        return df

    def get_symbol_prices(self, fund_family):
        symbols = fund_family["symbols"]
        if not symbols:
            return None

        prices = []
        for symbol_dict in symbols:

            # Find all dates we have first.
            last_lookup_day = self._find_last_lookup(symbol_dict["symbol"])
            if last_lookup_day[0] is None:
                # First time seeing this symbol.
                df_new = self.scrape(
                    symbol_dict["symbol"],
                    self.start_date,
                    self.end_date
                )
                self.db.insert_df(df_new, new=True, params=symbol_dict)
                df_new = self.add_columns_to_df(df_new, symbol_dict)
                prices.append(df_new)
                continue


            query = self.db.all_prices_query
            params = [symbol_dict["symbol"]]
            df = pd.read_sql_query(query, self.db.dbh, params=params)
            df = self.add_columns_to_df(df, symbol_dict)

            # 0 day difference - Just pull from db.
            if int(last_lookup_day[0]) <= 1:
                prices.append(df)
                continue
            start_date = missing_dates[1]
            df_new = self.scrape(symbol_dict["symbol"], start_date, self.end_date)
            if df_new is not None:
                df_new = self.add_columns_to_df(df_new, symbol_dict)
                self.db.insert_df(df_new, params=symbol_dict)
                df = pd.concat([df, df_new])

            prices.append(df)
        return pd.concat(prices)

    def insert_df(self, df, new=False, params={}):
        table = "symbol_dates"
        df.columns = [c.lower() for c in df.columns]

        if new:
            symbol = None
            name = None
            for k, v in params.items():
                if k == "symbol":
                    symbol = v
                if k == "name":
                    name = v
            if not symbol or not name:
                raise Exception("Invalid Symbol: {}".format(str(params)))
            self.db.insert_new_symbol(symbol, name)

        df.to_sql(table, self.db.dbh, if_exists="append")

    def logit(self, start, key, log_type):
        msg = None
        if log_type == "prices":
            msg = "{d} - Grabbed Prices for: {k}"
        if log_type == "error":
            msg = "{d} - No prices found for: {k}"
        if not msg:
            return
        duration = "{:<10.4}".format(time() - start).strip()
        print(msg.format(d=duration, k=key))

    def run_all(self):
        self.fund_families = self.get_fund_families()
        for key, ff in self.fund_families.items():
            self.fund_families[key]["fund_page"] = self.get_fund_page(ff)
            self.fund_families[key]["symbols"] = (
                self.get_all_symbols(self.fund_families[key])
            )

            start = time()
            self.fund_families[key]["prices"] = (
                self.get_symbol_prices(self.fund_families[key])
            )
            if self.fund_families[key]["prices"] is not None:
              self.logit(start, key, "prices")
            else:
              self.logit(start, key, "error")

    def merge_symbols_to_daily(self, df, dataframe=False):
        df.reset_index(inplace=True)
        avgs = df.groupby(["date"])["close"].mean()
        if dataframe:
            return pd.DataFrame(avgs).reset_index()
        return avgs

    def _growth_rate(self, df):
        avgs = self.merge_symbols_to_daily(df)
        growth_rate = (avgs[-1] - avgs[0]) / avgs[0]
        return growth_rate

    def top_fund_families(self, n=5):
        n = min(n, len(self.fund_families))

        growth_rates = []
        for k in self.fund_families.keys():
            growth_rate = self._growth_rate(self.fund_families[k]["prices"])
            growth_rates.append({
                "fund_family": k,
                "growth_rate": growth_rate,
            })
        sort_key = lambda x: x["growth_rate"]
        return sorted(growth_rates, key=sort_key, reverse=True)[:n]

    def winners_losers(self, df):
        #XXX TODO `top_fund_families` should use this.

        unique_symbols = df.symbol.unique()
        size = 5
        if len(unique_symbols) <= size * 2:
            return df

        close = "close"
        d = "date"
        gr = "growth_rate"
        min_date = "min_date"
        max_date = "max_date"
        n = "name"
        s = "symbol"

        def min_max_dates(x):
            names = {
                min_date: np.min(x[d]),
                max_date: np.max(x[d]),
            }
            return pd.Series(names)

        def back_to_datetime64(df, column):
            df[column] = df[column].astype(np.datetime64)
            return df

        def growth_rate(x):
            return (x["close_x"] - x["close_y"]) / x["close_x"]

        def winner_loser_text(df, gr, winner_or_loser):
            mapper = {u: None for u in df[gr].unique()}
            for m in mapper:
                percent = df[df[gr] == m][gr].values[0]
                percent = round(percent * 100, 1)
                mapper[m] = "{}: {}%".format(winner_or_loser, percent)
            return mapper


        df_min_max = pd.DataFrame(
            df.groupby([s,n]).apply(min_max_dates)
        ).reset_index()

        df = back_to_datetime64(df, d)
        df_min_max = back_to_datetime64(df_min_max, min_date)
        df_min_max = back_to_datetime64(df_min_max, max_date)

        interested_columns = [close, d, n, s]

        # Match to min_date => Close_x
        df_min_max = (
            df_min_max.merge(
                df[interested_columns], left_on=[s, max_date], right_on=[s, d])
        )
        # Match to max_date => Close_y
        df_min_max = (
            df_min_max.merge(
                df[interested_columns], left_on=[s, min_date], right_on=[s, d])
        )
        df_min_max[gr] = df_min_max.apply(growth_rate, axis=1)

        df_top_5 = df_min_max.nlargest(size, gr)
        map_text = winner_loser_text(df_top_5, gr, "winner")
        df_top_5["winner"] = df_top_5[gr].map(map_text)

        df_bottom_5 = df_min_max.nsmallest(size, gr)
        map_text = winner_loser_text(df_bottom_5, gr, "loser")
        df_bottom_5["winner"] = df_bottom_5[gr].map(map_text)

        keep = [gr, n, s, "winner"]
        df_min_max = pd.concat((df_top_5, df_bottom_5))[keep]

        nx = n + "_x"
        keep = [d, close, gr, nx, s, "winner"]
        df = df.merge(df_min_max, left_on=[s], right_on=[s])
        return df[keep]

    def combine_dataframes(self, dfs):
        return pd.concat(dfs)

    def list_all_fund_families(self):
        table = self._load_fund_families_table()
        ff = self._find_all_fund_families(table)
        return sorted(ff.keys())
