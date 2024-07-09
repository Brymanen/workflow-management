"""
Running this script creates a database and a table containing some basic
keywords.
"""

import sqlite3


def create_keyword_mappings():
    dbname = "workflow_db.sqlite"
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    try:
        table_name = "keyword_mapping"
        create_table_content(table_name, conn, cur)
    except sqlite3.OperationalError:
        cur.execute(
            f'CREATE TABLE "{table_name}" '
            f'(routing_key TEXT, keyword TEXT)'
        )
        create_table_content(table_name, conn, cur)


def create_table_content(table_name, conn, cur):
    routing_key = "content"
    keywords = ["600004", "600015", "600025", "4004", "800001", "600003"]
    for keyword in keywords:
        cur.execute(
            f'INSERT INTO "{table_name}" '
            f'(routing_key, keyword) '
            f'values ("{routing_key}", "{keyword}")'
        )
    conn.commit()
    conn.close()


create_keyword_mappings()
