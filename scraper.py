from bs4 import BeautifulSoup
import datetime
from dateutil.relativedelta import relativedelta
import re
import requests_cache
import requests

from requests.exceptions import ConnectionError

import pandas as pd
import pandas_datareader.data as web

from time import time

from utils import (
    cache_path,
    combine_dataframes,
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
#XXX TODO
# cast start_date, end_date to be week basis so we actually cache.

class MFScraper:
    def __init__(self, ds, cache_path, cache_expire_days,
                 start, end):
        self.ds=ds
        self.cache_expire_days=datetime.timedelta(days=cache_expire_days)
        self.session = requests_cache.CachedSession(cache_name=cache_path,
                                                    backend="sqlite",
                                                    expire_after=self.cache_expire_days)

        self.start = start
        self.end = end
        self.ignore = {
            "families": [
                "TOPS",
                "Symmetry Partners",
                "AXA Equitable",
            ],
            "symbols": [
                "HYPPX",
            ]
        }

    def scrape(self, symbol):
        try:
            response = web.DataReader(symbol, self.ds, self.start,
                                      self.end, session=self.session)
            return response
        except KeyError:
            print(
                "Could not retrieve prices for: {}, using {}"
                .format(symbol,self.ds)
            )
            return None

    def get_fund_families(self, limit=[]):
        self._ensure_pickle(FUND_FAMILIES)
        content = load_pickled_page(FUND_FAMILIES)
        soup = soupit(content)
        table = soup.find("table")

        if limit:
            fund_families = self._find_specific_fund_families(table, limit)
        else:
            fund_families = self._find_all_fund_families(table)
        return fund_families

    def _find_specific_fund_families(self, table, limit):
        fund_families = dict()
        for link in table.find_all("a"):
            link_name = link.get_text()
            if link_name:
                for l in limit:
                    if re.match(link_name.lower(), l):
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

    def get_symbol_prices(self, fund_family):
        symbols = fund_family["symbols"]
        if not symbols:
            return None
        prices = []
        for symbol in symbols:
            df = self.scrape(symbol["symbol"])
            if df is None:
                continue
            df["symbol"] = symbol["symbol"]
            df["name"] = symbol["name"]
            prices.append(df)
        return combine_dataframes(prices)

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

    def run_all(self, limit=[]):
        self.fund_families = self.get_fund_families(limit=limit)
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
        avgs = df.groupby(["Date"])["Close"].mean()
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

