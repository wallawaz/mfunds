# coding: utf8
from contextlib import contextmanager
import sqlite3

jday = lambda x: "julianday(%s)" % x

class DB:

    def __init__(self, path):
        self.path = path
        self.dbh = sqlite3.connect(path)
        self.create_tables()

    @classmethod
    def tables(cls):
        T = "TEXT"
        D = "DATE"
        R = "REAL"
        CD = "CURRENT_DATE"
        return {
            "mutual_funds": {
                "pk": ("symbol", "fund_family"),
                "columns": [
                    ("symbol", T),
                    ("fund_family", T),
                    ("name", T),
                ]
            },
            "symbol_lookups": {
                "pk": ("symbol", "date"),
                "fk": [("symbol", "mutual_funds", "symbol"),],
                "columns": [
                    ("symbol", T),
                    ("date", D, CD),
                ]
            },
            "mutual_fund_prices": {
                "pk": ("symbol", "date"),
                "fk": [("symbol", "mutual_funds", "symbol"),],
                "columns": [
                    ("symbol", T),
                    ("date", D),
                    ("high", R),
                    ("low", R),
                    ("open", R),
                    ("close", R),
                    ("volume", R),
                ]
            }
        }

    @classmethod
    def foreign_keys(cls, fks):
        definition = "FOREIGN KEY ({ca}) REFERENCES {tb} ({cb})"
        out = []
        while fks:
            f = fks.pop(0)
            out.append(definition.format(ca=f[0], tb=f[1], cb=f[2]))
        return ",".join(out)

    @classmethod
    def create_statement(cls, table, definition):
        formatter = {
            "table": table,
            "columns": "",
            "pk": "",
            "fk": "",
        }
        statement = (
            "CREATE TABLE IF NOT EXISTS {table} (\n"
            "    {columns}\n"
            "    {pk}\n"
            "    {fk}\n"
            ");"
        )
        columns = []
        for c in definition["columns"]:
            col = c[0] + " " + c[1]
            if len(c) > 2:
                col += " DEFAULT " + c[2]
            columns.append(col)

        formatter["columns"] = ",".join(columns)

        pk = definition.get("pk", "")
        fk = definition.get("fk", "")

        if pk:
            formatter["columns"] += ","
            pk = ",".join(pk)
            pk = "PRIMARY KEY (%s)" % pk
        formatter["pk"] = pk

        if fk:
            formatter["fk"] = DB.foreign_keys(fk)

        if formatter["pk"] and formatter["fk"]:
            formatter["pk"] += ","

        return statement.format(**formatter)

    @contextmanager
    def cursor_execute(self, sql, params=[]):
        curr = self.dbh.cursor()
        curr.execute(sql, params)
        self.dbh.commit()
        yield curr
        curr.close()

    def create_tables(self):
        tables = DB.tables()
        for t in tables:
            statement = DB.create_statement(t, tables[t])
            #for statement in statements:
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

    def insert_new_mf(self, symbol=None, fund_family=None, name=None):
        query = (
            "INSERT INTO mutual_funds (symbol, fund_family, name) "
            "VALUES (?,?,?)"
        )
        with self.cursor_execute(query, params=[symbol, fund_family, name]) as curr:
            _ = curr.rowcount

    def clean_column_names(self, columns):
        cleaned = []
        for c in columns:
            c = c.lower()
            c = "_".join(c.split())
            cleaned.append(c)
        return cleaned

    def insert_df(self, df, new=False, params={}):
        if params.get("symbol") is None:
            raise Exception("Invalid Symbol: {}".format(str(params)))

        if new:
            self.insert_new_mf(**params)

        df["symbol"] = params["symbol"]
        df.reset_index(inplace=True)

        df.columns = self.clean_column_names(df.columns)
        table = "mutual_fund_prices"

        db_def = DB.tables()[table]
        keep_columns = []
        for c in db_def["columns"]:
            keep_columns.append(c[0])

        idx = []
        for c in db_def["pk"]:
            idx.append(c)

        df = df[keep_columns]
        df.set_index(idx, inplace=True)
        df.to_sql(table, self.dbh, if_exists="append")

    @property
    def all_prices_query(self):
        return "SELECT * FROM mutual_fund_prices WHERE symbol = ?"
