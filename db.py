# coding: utf8
from contextlib import contextmanager
import sqlite3

jday = lambda x: "julianday(%s)" % x

class DB:

    def __init__(self, path):
        self.path = path
        self.dbh = sqlite3.connect(path)
        self.create_tables()

    @contextmanager
    def cursor_execute(self, sql, params=[]):
        curr = self.dbh.cursor()
        curr.execute(sql, params)
        self.dbh.commit()
        yield curr
        curr.close()

    def create_tables(self):
        statements = ["""
            CREATE TABLE IF NOT EXISTS mutual_funds (
                symbol TEXT,
                name TEXT,
                PRIMARY KEY (symbol)
            )""",
            """
            CREATE TABLE IF NOT EXISTS symbol_lookups (
                symbol TEXT,
                date DATE DEFAULT CURRENT_DATE,
                PRIMARY KEY (symbol, date)
                FOREIGN KEY (symbol) REFERENCES mutual_funds (symbol)
            )""",
            """
            CREATE TABLE IF NOT EXISTS mutual_fund_prices (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                high REAL,
                low REAL,
                open REAL,
                close REAL,
                volume REAL,
                adj_close REAL,
                PRIMARY KEY (symbol, date),
                FOREIGN KEY (symbol) REFERENCES mutual_funds (symbol)
            )
            """
        ]
        for statement in statements:
            with self.cursor_execute(statement) as curr:
                _ = curr.rowcount

    def log_symbol_lookup(self, symbol):
        query = "INSERT INTO symbol_lookups (symbol) VALUES (?)"

        with self.cursor_execute(query, params=[symbol]) as curr:
            _ = curr.rowcount

    def last_symbol_lookup(self, symbol):
        current_date = jday("CURRENT_DATE")
        latest_lookup = jday("MAX(date)")
        max_date = "MAX(date)"

        query = (
            "SELECT "
                "{c} - {l} as diff , "
                "{m} as max_date "
            "FROM "
            "   symbol_lookups "
            "WHERE "
            "   symbol = ?"
        ).format(c=current_date, l=latest_lookup, m=max_date)
        with self.cursor_execute(query, params=[symbol]) as curr:
            return curr.fetchone()

    def insert_new_mf(self, symbol, name):
        query = "INSERT INTO mutual_funds (symbol, name) VALUES (?,?)"
        with self.cursor_execute(query, params=[symbol, name]) as curr:
            _ = curr.rowcount

    def clean_column_names(self, columns):
        cleaned = []
        for c in columns:
            c = c.lower()
            c = "_".join(c.split())
            cleaned.append(c)
        return cleaned


    def insert_df(self, df, new=False, params={}):
        if params.get("symbol") is None or params.get("name") is None:
            raise Exception("Invalid Symbol: {}".format(str(params)))

        if new:
            self.insert_new_mf(params["symbol"], params["name"])

        table = "mutual_fund_prices"

        df.columns = self.clean_column_names(df.columns)

        df["symbol"] = params["symbol"]
        df.to_sql(table, self.dbh, if_exists="append")

    @property
    def all_prices_query(self):
        return "SELECT * FROM mutual_fund_prices WHERE symbol = ?"
