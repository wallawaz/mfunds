# coding: utf8
import argparse
from bs4 import BeautifulSoup

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt

from dash.dependencies import Input, Output

from datetime import datetime
from dateutil.relativedelta import relativedelta

import re
import requests
import sys
import plotly.graph_objs as go
from urllib.error import HTTPError
import time

from scraper import MFScraper

from utils import (
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


def get_app(header, mf_scraper):
    app = dash.Dash(sharing=True, csrf_protect=False)

    # inital layout
    app.layout = get_app_layout(header, mf_scraper)

    @app.callback(
        Output("graph-mf", "figure"),
        [Input("ff-id", "value")])
    def update_figure(selected_family):

        color_green = "#088c31"
        color_red = "#FC6955"

        selected_ff = mf_scraper.fund_families[selected_family]
        selected_df = selected_ff["prices"]

        selected_df = mf_scraper.winners_losers(selected_df)

        sorted_symbols = []
        for s in selected_df.symbol.unique():
           gr = selected_df[selected_df.symbol == s]["growth_rate"].values[0]
           sorted_symbols.append((gr, s))

        sorted_symbols.sort(key=lambda x: x[0], reverse=True)

        funds = []
        for gr, i in sorted_symbols:
            df_symbol = selected_df[selected_df.symbol == i]

            winner = selected_df[selected_df.symbol == i]["winner"].values[0]
            if winner[0] == "w":
                color = color_green
            else:
                color = color_red

            funds.append(
                go.Scatter(
                    x=df_symbol["date"],
                    y=df_symbol["close"],
                    text=df_symbol["name_x"] + "<br>" + df_symbol["winner"],
                    mode="lines",
                    name=i,
                    marker=dict(
                        color=color
                    ),
                )
            )
        return {
            "data": funds,
            "layout": time_series_layout(),
        }

    return app

def get_mf_scraper(limit, db_path):

    ds = "yahoo"
    cache_name = cache_path()

    # 7 day cache expiration.
    start_date, end_date = get_start_and_end_dates()
    mf_scraper = MFScraper(db_path, ds, cache_name, 7, start_date, end_date,
                           limit=limit)
    return mf_scraper

def load_mf_scraper_with_df(mf_scraper):

    mf_scraper.run_all()
    mf_scraper.top_fund_families = mf_scraper.top_fund_families()

    out = []
    for f in mf_scraper.top_fund_families:
        ff = mf_scraper.fund_families[f["fund_family"]]
        df = ff["prices"]
        df = mf_scraper.merge_symbols_to_daily(df, dataframe=True)
        df["fund_family"] = f["fund_family"]
        out.append(df)

    mf_scraper.df_all = mf_scraper.combine_dataframes(out)
    return mf_scraper

def time_series_graphes(df):
    graphs = [
        go.Scatter(
            x=df[df.fund_family == i]["date"],
            y=df[df.fund_family == i]["close"],
            text=i,
            name=i,
            mode="lines",
        ) for i in df.fund_family.unique()
    ]
    return graphs

def time_series_layout():
	return go.Layout(
		xaxis={"type": "date", "title": "Date"},

		yaxis=go.layout.YAxis(
            title="Closing Price",
            automargin=True,
        ),
		legend={'x': 0, 'y': 1},
		hovermode='closest',
        autosize=True,
        height=900,
	)

def get_datatable(df):
	df = df[["symbol", "close"]]
	return df.groupby(df.symbol).mean().reset_index()

def get_app_layout(header, mf_scraper):
    ts_graphs = time_series_graphes(mf_scraper.df_all)
    ts_layout = time_series_layout()
    fund_families = [
        {"label": i, "value": i} for i in mf_scraper.fund_families.keys()
    ]

    l = html.Div([
            html.H4(header),
            html.Label("Specific Fund Family"),
            dcc.Dropdown(
                id="ff-id",
                options=fund_families,
                #multi=True
            ),
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

if __name__ == "__main__":
    inital_header = "Top Fund Families by Growth Rate"

    list_desc = "List all fund families and exit."
    db_desc = "Path for sqlite db storing pricing info."

    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", nargs="+")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--list", help=list_desc, action="store_true")
    parser.add_argument("--db", help=db_desc, default="data/mf.sqlite")
    args = parser.parse_args()

    mf_scraper = get_mf_scraper(args.limit, args.db)
    if args.list:
        print("All Fund Families Available:")
        for ff in mf_scraper.list_all_fund_families():
            print(ff)
        sys.exit(0)

    mf_scraper = load_mf_scraper_with_df(mf_scraper)

    print("done grabbing dataframes.")
    print("loading dash app")

    app = get_app(inital_header, mf_scraper)
    app.run_server(debug=args.debug)
