# coding: utf8
import datetime
from json.decoder import JSONDecodeError
import pandas as pd
import pandas_datareader as pdr
import pickle
import os

TINGO_API_KEY = os.getenv("TINGO_API_KEY")
START_DATE = datetime.date(2015,1,1)

def cache_path():
    curr_path = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(
        curr_path,
        "data",
        "cache",
        "symbols"
    )

def pickle_path(url):
    curr_path = os.path.dirname(os.path.realpath(__file__))
    url = url.replace("/","_")
    return os.path.join(
        curr_path,
        "data",
        url + ".pickle"
    )

def get_tingo_weekly(symbol):
    try:
        df = pdr.get_data_tiingo(symbol, api_key=TINGO_API_KEY)
    except JSONDecodeError:
        df = None
    return df

def df_weekly_to_quarterly(df, date_column, addional_indexes=["symbol"],
                           stats_cols=["close"]):
    #df.reset_index(inplace=True)
    #df.set_index(index, inplace=True)

	df = df[ [date_column]+addional_indexes+stats_cols ]

	df["quarter"] = pd.PeriodIndex(df[date_column], freq="Q")
	df.set_index("quarter", inplace=True)

	df = df.to_timestamp().reset_index()
	df = df.groupby([df.quarter] + addional_indexes)[stats_cols].mean()
	return df.reset_index()

def clean_df(df):
	df = df_weekly_to_quarterly(df, "date")
	return df

def pickle_response(response):
    fn = pickle_path(response.url)
    with open(fn, "wb") as fp:
        pickle.dump(response.content, fp)

def pickled_page_exists(url):
    fn = pickle_path(url)
    return os.path.exists(fn)

def load_pickled_page(url):
    fn = pickle_path(url)
    with open(fn, "rb") as f:
        data = pickle.load(f, encoding="utf-8")
    return data

def get_start_and_end_dates():
    """For consistent caching"""

    today = datetime.datetime.now().date()
    dow = today.weekday()
    end_date = today - datetime.timedelta(days=dow)
    start_date = START_DATE
    return (start_date, end_date)

