import sqlite3


def clean_db(uri):
    with sqlite3.connect(uri) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS pokemon(
        id INTEGER PRIMARY KEY,
        name TEXT, 
        base_stat integer)"""
                    )
        con.execute("DELETE FROM pokemon")
