#!/usr/bin/python
# -*- coding: utf-8 -*-

# retrieving_data.py

from __future__ import print_function

import pandas as pd
import MySQLdb as mdb


if __name__ == "__main__":
    # Connect to the MySQL instance
    db_host = 'localhost'
    db_user = 'sec_user'
    db_pass = 'Quantum1~~2004'
    db_name = 'securities_master'
    con = mdb.connect(db_host, db_user, db_pass, db_name)

    #Select all of the historic Google adjusted close data
    sql = """SELECT dp.price_date, dp.adj_close_price
             FROM symbol AS sym
             INNER JOIN daily_price AS dp
             ON dp.symbol_id = sym.id
             WHERE sym.ticker = 'NXPI'
             ORDER BY dp.price_date ASC;"""

    # sql = """
    # SELECT
    # dp.price_date,
    # dp.adj_close_price
    # FROM symbol AS sym
    # INNER JOIN daily_price AS dp
    # ON dp.symbol_id = sym.id
    # WHERE
    # sym.ticker    = 'NXPI'
    # AND dp.price_date BETWEEN '2015-06-09' AND '2015-06-15'
    # ORDER BY dp.price_date ASC;
    # """


    # Create a pandas dataframe from the SQL query
    nxpi = pd.read_sql_query(sql, con=con, index_col='price_date')    

    # Output the dataframe tail
    print(nxpi.tail())
