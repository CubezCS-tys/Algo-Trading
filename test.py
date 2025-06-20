#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler

import warnings
import MySQLdb as mdb
import MySQLdb.cursors
import yfinance as yf
import pandas as pd
from pprint import pprint

# ——— Configuration ———
DB_HOST = 'localhost'
DB_USER = 'sec_user'
DB_PASS = 'Quantum1~~2004'
DB_NAME = 'securities_master'

# ——— DB Connection ———
def get_db_connection():
    return mdb.connect(
        host=DB_HOST,
        user=DB_USER,
        passwd=DB_PASS,
        db=DB_NAME,
        autocommit=True,
        #cursorclass=MySQLdb.cursors.DictCursor
    )

# ——— Load tickers ———
def load_tickers():
    conn = get_db_connection()
    cur  = conn.cursor()
    cur.execute("SELECT id, ticker FROM symbol ORDER BY id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows  # list of (id, ticker) 


if __name__ == "__main__":
    # pprint(load_tickers())
    # rows       = load_tickers()                # e.g. [(1, 'AAPL'), (2, 'BRK.B'), …]
    # ids, ticks = zip(*rows)                    # ids = (1, 2, …), ticks = ('AAPL', 'BRK.B', …)
    # pprint(ticks)
    # api_tickers = [t.replace('.', '-') for t in ticks]
    
    rows = load_tickers()    # now rows is [(1,'AAPL'), (2,'MSFT'), …]
    ids, ticks = zip(*rows)  # works as you originally expected
    pprint(ids)
    pprint(ticks)