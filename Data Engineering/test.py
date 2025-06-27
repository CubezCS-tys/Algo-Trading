#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# import os
# import datetime
# import logging
# from logging.handlers import TimedRotatingFileHandler

# import warnings
# import MySQLdb as mdb
# import MySQLdb.cursors
# import yfinance as yf
# import pandas as pd
# from pprint import pprint

# # ——— Configuration ———
# DB_HOST = 'localhost'
# DB_USER = 'sec_user'
# DB_PASS = 'Quantum1~~2004'
# DB_NAME = 'securities_master'

# # ——— DB Connection ———
# def get_db_connection():
#     return mdb.connect(
#         host=DB_HOST,
#         user=DB_USER,
#         passwd=DB_PASS,
#         db=DB_NAME,
#         autocommit=True,
#         #cursorclass=MySQLdb.cursors.DictCursor
#     )

# # ——— Load tickers ———
# def load_tickers():
#     conn = get_db_connection()
#     cur  = conn.cursor()
#     cur.execute("SELECT id, ticker FROM symbol ORDER BY id")
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#     return rows  # list of (id, ticker) 


# if __name__ == "__main__":
#     # pprint(load_tickers())
#     # rows       = load_tickers()                # e.g. [(1, 'AAPL'), (2, 'BRK.B'), …]
#     # ids, ticks = zip(*rows)                    # ids = (1, 2, …), ticks = ('AAPL', 'BRK.B', …)
#     # pprint(ticks)
#     # api_tickers = [t.replace('.', '-') for t in ticks]
    
#     rows = load_tickers()    # now rows is [(1,'AAPL'), (2,'MSFT'), …]
#     ids, ticks = zip(*rows)  # works as you originally expected
#     api_tickers = [t.replace('.', '-') for t in ticks]
#     pprint(ids)
#     print(api_tickers)

import logging
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
print(dir_path)
filename = os.path.join(dir_path, 'test_log.log')
print(filename)

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler(filename)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

def do_logging():
    logger.info("test")


if __name__ == '__main__':
    do_logging()