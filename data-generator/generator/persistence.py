import sqlite3
from typing import Sequence

class Persistence:
    con = None
    cur = None

    @classmethod
    def initialize(cls, database_path: str):
        Persistence.con = sqlite3.connect(database_path)
        Persistence.cur = Persistence.con.cursor()

    @classmethod
    def execute(cls, query: str, parameters: Sequence) -> None:
        Persistence.cur.execute(query, parameters)
        Persistence.con.commit()

    @classmethod
    def ddl(cls, query: str) -> None:
        Persistence.cur.execute(query)
        Persistence.con.commit()

    @classmethod
    def query(cls, query) -> list[tuple]:
        res = Persistence.cur.execute(query)
        records = res.fetchall()
        return records

    @classmethod
    def close(cls) -> None:
        Persistence.cur.close()
        Persistence.con.commit()
        Persistence.con.close()
