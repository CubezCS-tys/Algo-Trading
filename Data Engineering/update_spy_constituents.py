#!/usr/bin/env python3
import datetime
import pandas as pd
import MySQLdb as mdb


DB = dict(host='localhost', user='sec_user', passwd='Quantum1~~2004', db='securities_master')

def fetch_spy_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    df = pd.read_html(url, header=0)[0]
    return df.Symbol.str.replace(r'\.', '-', regex=True).tolist()

def update_snapshot():
    today = datetime.date.today()
    # use the first of this month as the snapshot key
    snapshot = today.replace(day=1)
    tickers = fetch_spy_tickers()
    
    con = mdb.connect(**DB)
    cur = con.cursor()
    # clear out any old rows for this month
    cur.execute("DELETE FROM spy_constituents WHERE snapshot_month = %s", (snapshot,))
    
    # insert each ticker if it exists in your symbol table
    for t in tickers:
        cur.execute("SELECT id FROM symbol WHERE ticker = %s", (t,))
        row = cur.fetchone()
        if row:
            symbol_id = row[0]
            cur.execute("""
                INSERT INTO spy_constituents (snapshot_month, symbol_id)
                VALUES (%s, %s)
            """, (snapshot, symbol_id))
    con.commit()
    cur.close()
    con.close()

if __name__ == "__main__":
    update_snapshot()
