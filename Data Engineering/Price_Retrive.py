
# #!/usr/bin/env python3
# # -*- coding: utf-8 -*-

# """
# price_retrieval.py

# Retrieve historical pricing data from Yahoo Finance using yfinance
# and load it into a MySQL database, with robust handling of single- vs multi-ticker downloads
# and fallback for missing adjusted-close columns.
# """

# import datetime
# import warnings

# import MySQLdb as mdb
# import yfinance as yf
# import pandas as pd

# # ——— DB CONNECTION ———
# DB_HOST = 'localhost'
# DB_USER = 'sec_user'
# DB_PASS = 'Quantum1~~2004'
# DB_NAME = 'securities_master'


# def get_db_connection():
#     """Returns a new MySQLdb connection"""
#     return mdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)


# # ——— TICKER LIST ———

# def obtain_list_of_db_tickers():
#     """
#     Fetch (id, ticker) tuples from your symbol table.
#     """
#     con = get_db_connection()
#     with con:
#         cur = con.cursor()
#         cur.execute("SELECT id, ticker FROM symbol")
#         return cur.fetchall()


# # ——— YFINANCE DOWNLOAD ———

# def fetch_history_batch(tickers, start_date, end_date):
#     """
#     Download OHLC+Adj Close+Volume for a list of tickers in one request.
#     Returns a dict: { ticker: DataFrame, ... }
#     """
#     # Perform download
#     raw = yf.download(
#         tickers=tickers,
#         start=start_date,
#         end=end_date,
#         progress=False,
#         threads=True,
#         group_by='ticker'
#     )

#     results = {}
#     # Single-ticker: may come back as MultiIndex columns
#     if len(tickers) == 1:
#         df = raw.copy()
#         if isinstance(df.columns, pd.MultiIndex):
#             # flatten to single level
#             df.columns = df.columns.droplevel(0)
#         results[tickers[0]] = df
#     else:
#         # Multi-ticker: split by top-level
#         for t in tickers:
#             if t in raw.columns.get_level_values(0):
#                 sub = raw[t].dropna(how='all')
#                 results[t] = sub
#             else:
#                 results[t] = None
#     return results


# # ——— INSERT INTO DB ———

# def insert_daily_data_into_db(data_vendor_id, symbol_id, df):
#     """
#     Insert daily pricing data into `daily_price` table.

#     df: DataFrame indexed by date, cols must include:
#         Open, High, Low, Close, Volume,
#         and either Adj Close or (fallback) Close.
#     """
#     if df is None or df.empty:
#         return

#     # Ensure adjusted-close exists
#     if 'Adj Close' not in df.columns:
#         warnings.warn(
#             f"'Adj Close' column not found for symbol_id={symbol_id}, using 'Close' as fallback."
#         )
#         df['Adj Close'] = df['Close']

#     now = datetime.datetime.utcnow()
#     records = []
#     for date, row in df.iterrows():
#         try:
#             records.append((
#                 data_vendor_id,
#                 symbol_id,
#                 date.to_pydatetime(),
#                 now, now,
#                 float(row['Open']),
#                 float(row['High']),
#                 float(row['Low']),
#                 float(row['Close']),
#                 int(row['Volume']),
#                 float(row['Adj Close'])
#             ))
#         except KeyError as ke:
#             raise KeyError(
#                 f"Missing column {ke} for symbol_id={symbol_id}. Available: {list(df.columns)}"
#             )

#     cols = (
#         "data_vendor_id, symbol_id, price_date, created_date,"
#         " last_updated_date, open_price, high_price,"
#         " low_price, close_price, volume, adj_close_price"
#     )
#     placeholders = ", ".join(["%s"] * 11)
#     sql = f"INSERT INTO daily_price ({cols}) VALUES ({placeholders})"

#     con = get_db_connection()
#     with con:
#         cur = con.cursor()
#         cur.executemany(sql, records)
#         con.commit()


# # ——— MAIN LOOP ———

# if __name__ == "__main__":
#     warnings.filterwarnings('ignore')

#     tickers = obtain_list_of_db_tickers()
#     total = len(tickers)

#     batch_size = 50
#     start_date = "2000-01-01"
#     # Include end_date as tomorrow to be inclusive
#     end_date = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

#     for start in range(0, total, batch_size):
#         batch = tickers[start:start+batch_size]
#         ids, symbols = zip(*batch)
#         print(f"Downloading batch {start+1}-{start+len(batch)} of {total}...")
#         hist = fetch_history_batch(symbols, start_date, end_date)

#         for symbol_id, ticker in batch:
#             df = hist.get(ticker)
#             if df is None or df.empty:
#                 print(f"  • No data for {ticker}, skipping.")
#                 continue

#             try:
#                 insert_daily_data_into_db(1, symbol_id, df)
#                 print(f"  ✓ Inserted {len(df)} rows for {ticker}")
#             except Exception as e:
#                 print(f"  ✗ Error inserting {ticker}: {e}")

#     print("Done adding Yahoo Finance data to DB.")


#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
price_retrieval_fixed.py

Retrieve historical pricing data from Yahoo Finance using yfinance
and load it into a MySQL database, mapping tickers with dots to
Yahoo’s expected dash format (e.g. BRK.B → BRK-B).
"""

import datetime
import warnings

import MySQLdb as mdb
import yfinance as yf
import pandas as pd

# ——— DB CONNECTION ———
DB_HOST = 'localhost'
DB_USER = 'sec_user'
DB_PASS = 'Quantum1~~2004'
DB_NAME = 'securities_master'

def get_db_connection():
    """Returns a new MySQLdb connection"""
    return mdb.connect(host=DB_HOST, user=DB_USER, passwd=DB_PASS, db=DB_NAME)


# ——— TICKER LIST ———

def obtain_list_of_db_tickers():
    """
    Fetch (id, ticker) tuples from your symbol table, ordered by id.
    """
    con = get_db_connection()
    with con:
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol ORDER BY id")
        return cur.fetchall()


# ——— YFINANCE DOWNLOAD ———

def fetch_history_batch(tickers, start_date, end_date):
    """
    Download OHLC+Adj Close+Volume for a list of tickers in one request.
    Returns a dict: { ticker: DataFrame, ... }
    """
    raw = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        progress=False,
        threads=True,
        group_by='ticker'
    )

    results = {}
    # Single-ticker: may come back as MultiIndex columns
    if len(tickers) == 1:
        df = raw.copy()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.droplevel(0)
        results[tickers[0]] = df
    else:
        # Multi-ticker: split by top-level
        for t in tickers:
            if t in raw.columns.get_level_values(0):
                sub = raw[t].dropna(how='all')
                results[t] = sub
            else:
                results[t] = None
    return results


# ——— INSERT INTO DB ———

def insert_daily_data_into_db(data_vendor_id, symbol_id, df):
    """
    Insert daily pricing data into `daily_price` table.
    """
    if df is None or df.empty:
        return

    # Ensure adjusted-close exists
    if 'Adj Close' not in df.columns:
        warnings.warn(
            f"'Adj Close' column not found for symbol_id={symbol_id}, using 'Close' as fallback."
        )
        df['Adj Close'] = df['Close']

    now = datetime.datetime.utcnow()
    records = []
    for date, row in df.iterrows():
        records.append((
            data_vendor_id,
            symbol_id,
            date.to_pydatetime(),
            now, now,
            float(row['Open']),
            float(row['High']),
            float(row['Low']),
            float(row['Close']),
            int(row['Volume']),
            float(row['Adj Close'])
        ))

    cols = (
        "data_vendor_id, symbol_id, price_date, created_date,"
        " last_updated_date, open_price, high_price,"
        " low_price, close_price, volume, adj_close_price"
    )
    placeholders = ", ".join(["%s"] * 11)
    sql = f"INSERT INTO daily_price ({cols}) VALUES ({placeholders})"

    con = get_db_connection()
    with con:
        cur = con.cursor()
        cur.executemany(sql, records)
        con.commit()


# ——— MAIN LOOP ———

if __name__ == "__main__":
    warnings.filterwarnings('ignore')

    tickers = obtain_list_of_db_tickers()
    total = len(tickers)

    batch_size = 50
    start_date = "2000-01-01"
    end_date = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()

    for start in range(0, total, batch_size):
        batch = tickers[start:start+batch_size]
        ids, symbols = zip(*batch)

        # *** Fix: convert any dots to dashes for Yahoo Finance ***
        api_tickers = [t.replace('.', '-') for t in symbols]

        print(
            f"Downloading batch {start+1}-{start+len(batch)} of {total} "
            f"({api_tickers[0]} … {api_tickers[-1]})"
        )
        hist = fetch_history_batch(api_tickers, start_date, end_date)

        # Now zip through original id/ticker and the mapped api_ticker
        for symbol_id, ticker, api_t in zip(ids, symbols, api_tickers):
            df = hist.get(api_t)
            if df is None or df.empty:
                print(f"  • No data for {ticker} (api: {api_t}), skipping.")
                continue

            try:
                insert_daily_data_into_db(1, symbol_id, df)
                print(f"  ✓ Inserted {len(df)} rows for {ticker}")
            except Exception as e:
                print(f"  ✗ Error inserting {ticker}: {e}")

    print("Done adding Yahoo Finance data to DB.")
