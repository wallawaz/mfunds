from contextlib import contextmanager
import sqlite3

class DB:

    def __init__(self, path):
        self.path = path
        self.dbh = sqlite3.connect(path)
        self.create_tables()

    @contextmanager
    def cursor_execute(self, sql, params=[]):
        curr = self.dbh.cursor()
        curr.execute(sql, params)
        yield curr
        curr.close()

    def create_tables(self):
        statements = ["""
            CREATE TABLE IF NOT EXISTS symbol_lookups (
                symbol text,
                ts timestamp DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, ts)
            )""",
        ]
        for statement in statements:
            with self.cursor_execute(statement) as curr:
                _ = curr.rowcount

    def log_symbol_lookup(self, symbol):
        query = "INSERT INTO symbol_lookups (symbol) VALUES (?)"

        with self.cursor_execute(query, params=[symbol]) as curr:
            _ = curr.rowcount

    def last_symbol_lookup(self, symbol):
        query = "SELECT ts FROM symbol_lookups WHERE symbol = ? ORDER BY ts DESC"

        with self.cursor_execute(query, params=[symbol]) as curr:
            return curr.fetchone()
