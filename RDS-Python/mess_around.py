#DB name: Alma_DB
#DB instance identifier: database-1
#username: admin
#pass: Ktr321ugh!
#host: database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com
#port:3306

import sqlite3
#from types import NoneType
#from types import NoneType
import pymysql 
import pandas as pd
#import grab_cmc_data
import numpy as np
import datetime



def drop_database(db, cursor):

    sql = '''drop database test_table'''
    cursor.execute(sql)

def create_database(db, cursor):

    sql = '''create database c_price_sheet'''
    cursor.execute(sql)

def select_database(db, cursor, table_name):

    sql = '''use c_price_sheet'''
    print(sql)
    cursor.execute(sql)

def test_1():
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    sql = '''SELECT * from web_app_1_hedgebot_results'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

test_1()