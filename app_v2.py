import argparse
from bs4 import BeautifulSoup

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt

from datetime import datetime
from dateutil.relativedelta import relativedelta

import re
import requests
import plotly.graph_objs as go
from urllib.error import HTTPError 
import time

from scraper import MFScraper

from utils import (
    combine_dataframes,
    cache_path,
    df_weekly_to_quarterly,
    feather_path,
    get_start_and_end_dates,
    load_pickled_page,
    pickled_page_exists,
    pickle_response,
    read_feather,
    clean_df,
    write_feather,
)

SLEEP_INTERVAL = 0.5

def get_app():
    return dash.Dash(sharing=True, csrf_protect=False)

def get_all_fund_families(limit):

    ds = "yahoo"
    cache_name = cache_path()

    # 7 day cache expiration.
    start_date, end_date = get_start_and_end_dates()
    mf_scraper = MFScraper(ds, cache_name, 7, start_date, end_date)

    mf_scraper.run_all(limit=limit)

    top_fund_families = mf_scraper.top_fund_families()
    out = []
    for f in top_fund_families:
        ff = mf_scraper.fund_families[f["fund_family"]]
        df = ff["prices"]
        df = mf_scraper.merge_symbols_to_daily(df, dataframe=True)
        df["fund_family"] = f["fund_family"] 
        out.append(df)

    df_all = combine_dataframes(out)
    return (top_fund_families, df_all)

def load_data_frame(d):
    if d["symbol"] is None:
        return None

    df = get_tingo_weekly(d["symbol"])
    if df is None:
        return None

    # Keep the full df for the time being..
    #df = df_weekly_to_quarterly(
    #    df,
    #    keep_cols=d,
    #)
    return df

def time_series_graphes(df):
    graphs = [
        go.Scatter(
            x=df[df.fund_family == i]["Date"],
            y=df[df.fund_family == i]["Close"],
            text=i,
            name=i,
            mode="lines",
        ) for i in df.fund_family.unique()
    ]
    return graphs

def time_series_layout(df):
    #margin={'l': 40, 'b': 40, 't': 10, 'r': 10, "autoexpand":True},
	return go.Layout(
		xaxis={"type": "date", "title": "Date"},

		yaxis=go.layout.YAxis(
            title="Closing Price",
            automargin=True,
        ),
		legend={'x': 0, 'y': 1},
		hovermode='closest',
        #style={"width": "100%", "height": "100%"},
        autosize=True,
        height=900,
	)

def get_datatable(df):
	df = df[["symbol", "close"]]
	return df.groupby(df.symbol).mean().reset_index()

def get_app_layout(header, df):
	ts_graphs = time_series_graphes(df)
	ts_layout = time_series_layout(df)

	#df_datatable = get_datatable(df)
	l = html.Div([
            html.H4(header),
            dcc.Graph(
                id="graph-mf",
				figure={
					"data": ts_graphs,
					"layout": ts_layout
				},
                style={"height": "100%"},
            ),
        ],
        className="container",
        style={"height": "100%"},
    )
    #dt.DataTable(
    #    rows=df_datatable.to_dict("records"),
    #    columns=df_datatable.columns,
    #    row_selectable=True,
    #    filterable=True,
    #    sortable=True,
    #    selected_row_indices=[],

    #    id="datatable-mf",
    #),
	return l

def logit(dataframes):
    l = len(dataframes)
    if l % 10 == 0:
        symbol = dataframes[-1].ix[0].symbol
        msg="{d} {s}: grabbed stats on {l} symbols".format(d=datetime.now(), s=symbol, l=l)
        print(msg)

def load_all_dataframes(limit):
    top_families, df = get_all_fund_families(limit)
    return top_families, df

if __name__ == "__main__":
    app = get_app()
    header = "MF Ranking"

    parser = argparse.ArgumentParser()
    #parser.add_argument("--no-cache", dest="no_cache", action="store_true")
    parser.add_argument("--limit", nargs="+")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    top_families, df_all = load_all_dataframes(args.limit)

    print("done grabbing dataframes.")
    app.layout = get_app_layout(header, df_all)

    app.run_server(debug=args.debug)
