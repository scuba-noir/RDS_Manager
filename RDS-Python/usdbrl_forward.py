
import numpy as np
import pandas as pd
import pdblp
import pymysql
from datetime import datetime
from dateutil.relativedelta import *


def create_term():
    con = pdblp.BCon(debug=False, port=8194, timeout=5000)
    con.start()
    today = datetime.today().strftime("%Y-%M-%D")
    start_date = "2000-01-01"
    ticker_ls = [
        'UC1  Curncy',
        'UC2 Curncy',
        'UC3 Curncy',
        'UC4 Curncy',
        'UC5 Curncy',
        'UC6 Curncy',
        'UC7 Curncy',
        'UC8 Curncy',
        'UC9 Curncy',
        'UC10 Curncy',
        'UC11 Curncy',
        'UC12 Curncy',
        'UC13 Curncy'
    ]

    data_df = con.bdh(ticker_ls, 'PX_LAST', start_date, today)
    print(data_df)
    con.stop()


create_term()