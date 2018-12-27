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
import pickle
import plotly.graph_objs as go

from urllib.error import HTTPError

import time
from utils import (
    combine_dataframes,
    df_weekly_to_quarterly,
    feather_path,
    get_tingo_weekly,
    load_pickled_page,
    pickled_page_exists,
    pickle_response,
    read_feather,
    clean_df,
    write_feather,
)

VANGUARD_FUND_PAGE = "http://quicktake.morningstar.com/fundfamily/vanguard/0C00001YUF/fund-list.aspx"
SLEEP_INTERVAL = 0.5

def get_app():
    return dash.Dash(sharing=True, csrf_protect=False)

def get_vanguard_symbols():
    symbol_regex = 't\=([A-Z]*)"'

    if not pickled_page_exists(VANGUARD_FUND_PAGE):
        print("page not found locally, grabbing.")
        response = requests.get(VANGUARD_FUND_PAGE)
        pickle_response(response)

    content = load_pickled_page(VANGUARD_FUND_PAGE)

    soup = BeautifulSoup(content, "html.parser")
    table = soup.find("table")
    cells = table.find_all("td", {"class": "msNormal"})

    seen = set()
    out = []
    for cell in cells:
        d = {
            "symbol": None,
            "name": cell.get_text()
        }
        href = cell.find("a")
        symbol = re.findall(symbol_regex, str(href))
        if symbol:
            if symbol[0] not in seen:
                d["symbol"] = symbol[0]
                out.append(d)
    return out

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
			x=df[df.symbol == i]["quarter"],
			y=df[df.symbol == i]["close"],
			name=i,
			mode="lines",
			#text=df["symbol"],
		) for i in df.symbol.unique()
	]
	return graphs

def time_series_layout(df):
	return go.Layout(
		xaxis={"type": "date", "title": "MF Closing Prices"},
		yaxis={"title": "Price"},
        #margin={'l': 40, 'b': 40, 't': 10, 'r': 10, "autoexpand":True},
		legend={'x': 0, 'y': 1},
		hovermode='closest',
        #style={"width": "100%", "height": "100%"},
        autosize=True,
	)

def get_datatable(df):
	df = df[["symbol", "close"]]
	return df.groupby(df.symbol).mean().reset_index()

def get_app_layout(header, df):
	ts_graphs = time_series_graphes(df)
	ts_layout = time_series_layout(df)

	df_datatable = get_datatable(df)
	l = html.Div([
            dcc.Graph(
                id="graph-mf",
				figure={
					"data": ts_graphs,
					"layout": ts_layout
				}
            ),
            #html.H4(header),
            #dt.DataTable(
            #    rows=df_datatable.to_dict("records"),
            #    columns=df_datatable.columns,
            #    row_selectable=True,
            #    filterable=True,
            #    sortable=True,
            #    selected_row_indices=[],

            #    id="datatable-mf",
            #),
            html.Div(id="selected-indexes"),
        ],
        className="container"
    )
	return l

def logit(dataframes):
    l = len(dataframes)
    if l % 10 == 0:
        symbol = dataframes[-1].ix[0].symbol
        msg="{d} {s}: grabbed stats on {l} symbols".format(d=datetime.now(), s=symbol, l=l)
        print(msg)

def _load_new_dataframes():
    dataframes = []
    for symbol_dict in get_vanguard_symbols():
        df = load_data_frame(symbol_dict)
        if df is None:
            continue
        dataframes.append(df)
        write_feather(symbol_dict["symbol"], df)
        logit(dataframes)
        time.sleep(SLEEP_INTERVAL)
    return dataframes

def _load_cache_dataframes():
    dataframes = []
    for symbol_dict in get_vanguard_symbols():
        fp = feather_path(symbol_dict["symbol"])
        df = read_feather(fp)
        if df is not None:
            dataframes.append(df)
    return dataframes

def load_all_dataframes(no_cache):
    if no_cache:
        print("reading new dfs")
        dataframes = _load_new_dataframes()
    else:
        print("reading cached dfs")
        dataframes = _load_cache_dataframes()
    return dataframes

if __name__ == "__main__":
    app = get_app()
    header = "MF Ranking"

    parser = argparse.ArgumentParser()
    parser.add_argument("--no-cache", dest="no_cache", action="store_true")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    dataframes = load_all_dataframes(args.no_cache)
    # Combine all <quarterly>? dataframes.
    df_all = combine_dataframes(dataframes)
    df_all = clean_df(df_all)

    print("done grabbing dataframes.")
    app.layout = get_app_layout(header, df_all)

    app.run_server(debug=args.debug)
