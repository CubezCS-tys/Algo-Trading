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

# ——— Configuration ———
DB_HOST = 'localhost'
DB_USER = 'sec_user'
DB_PASS = 'Quantum1~~2004'
DB_NAME = 'securities_master'

LOG_DIR = '/home/cubez/Desktop/Algo-Trading/logs'
os.makedirs(LOG_DIR, exist_ok=True)

# ——— Logging setup ———
logger = logging.getLogger('price_update')
logger.setLevel(logging.INFO)
handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, 'price_update.log'),
    when='midnight',
    interval=1,
    backupCount=7,
    encoding='utf-8'
)
handler.suffix = "%Y-%m-%d"
handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
))
logger.addHandler(handler)

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

# ——— Fetch latest ———
def fetch_latest(symbols):
    try:
        raw = yf.download(
            tickers=symbols,
            period="2d",           # fetch the last 2 days
            progress=False,
            threads=True,
            group_by='ticker'
        )
    except Exception as e:
        logger.error(f"Bulk download failed: {e}")
        # return None for every symbol so we don’t crash downstream
        return {t: None for t in symbols}

    out = {}
    multi = isinstance(raw.columns, pd.MultiIndex)

    for t in symbols:
        # slice out each ticker’s DataFrame if MultiIndex, else flat
        if multi:
            df = raw[t] if t in raw.columns.get_level_values(0) else None
        else:
            df = raw if t == symbols[0] else None

        # drop all-NaN rows, then take the very last one
        if df is None or df.dropna(how='all').empty:
            out[t] = None
        else:
            out[t] = df.dropna(how='all').tail(1)

    return out

# ——— Upsert + Audit ———
def insert_or_update(data_vendor_id, symbol_id, df):
    if df is None or df.empty:
        return

    if 'Adj Close' not in df.columns:
        df['Adj Close'] = df['Close']

    row = df.iloc[0]
    dt  = row.name.to_pydatetime()
    now = datetime.datetime.utcnow()

    conn = get_db_connection()
    cur  = conn.cursor()

    # fetch existing for audit
    cur.execute("""
        SELECT open_price, high_price, low_price,
               close_price, volume, adj_close_price
          FROM daily_price
         WHERE symbol_id=%s AND price_date=%s
    """, (symbol_id, dt.date()))
    existing = cur.fetchone()

    if existing:
        fields = [
            ('open_price',      float(row['Open'])),
            ('high_price',      float(row['High'])),
            ('low_price',       float(row['Low'])),
            ('close_price',     float(row['Close'])),
            ('volume',          int(row['Volume'])),
            ('adj_close_price', float(row['Adj Close']))
        ]
        for field_name, new_val in fields:
            old_val = existing[field_name]
            if old_val != new_val:
                cur.execute("""
                    INSERT INTO price_audit
                      (symbol_id, price_date, field_name, old_value, new_value)
                    VALUES (%s, %s, %s, %s, %s)
                """, (symbol_id, dt.date(), field_name, old_val, new_val))

    # upsert the row
    vals = (
        data_vendor_id, symbol_id, dt.date(),
        now, now,
        float(row['Open']), float(row['High']), float(row['Low']),
        float(row['Close']), int(row['Volume']), float(row['Adj Close'])
    )
    cur.execute("""
        INSERT INTO daily_price
          (data_vendor_id, symbol_id, price_date,
           created_date, last_updated_date,
           open_price, high_price, low_price,
           close_price, volume, adj_close_price)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          last_updated_date = VALUES(last_updated_date),
          open_price        = VALUES(open_price),
          high_price        = VALUES(high_price),
          low_price         = VALUES(low_price),
          close_price       = VALUES(close_price),
          volume            = VALUES(volume),
          adj_close_price   = VALUES(adj_close_price)
    """, vals)

    cur.close()
    conn.close()

# ——— Main ———
if __name__ == "__main__":
    warnings.filterwarnings('ignore')
    logger.info("Starting price update run")

    rows        = load_tickers()
    ids, ticks  = zip(*rows)
    api_tickers = [t.replace('.', '-') for t in ticks]

    logger.info(f"Fetching latest prices for {len(ids)} tickers")
    hist = fetch_latest(api_tickers)

    for sym_id, raw_tk, api_tk in zip(ids, ticks, api_tickers):
        df = hist.get(api_tk)
        if df is None or df.empty:
            logger.warning(
                f"No data for symbol_id={sym_id} ticker={raw_tk!r} (api={api_tk})"
            )
        else:
            insert_or_update(1, sym_id, df)
            dt_str = df.index[0].strftime("%Y-%m-%d")
            logger.info(f"{raw_tk} @ {dt_str}")

    logger.info("Price update run complete")

