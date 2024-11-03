#!/usr/bin/env python3
import pytds

# Database connection parameters
DRIVER = "ODBC Driver 17 for SQL Server"
DB_HOST = "localhost"
DB_USER = "sa"
DB_PASS = "Abcd@1234"
DB_NAME = "AggregateDB"

with pytds.connect(DB_HOST, DB_NAME, DB_USER, DB_PASS) as conn:
    cursor= conn.cursor()
    cursor.execute("select * from Tx_Curr_AHD_LineList")
    for row in cursor.fetchall():
        print(row)
