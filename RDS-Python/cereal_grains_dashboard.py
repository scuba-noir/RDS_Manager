# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 01:50:05 2021

@author: ChristopherTHOMPSON
"""
import numpy as np
import pandas as pd
import pdblp
import pymysql
from datetime import date
from dateutil.relativedelta import *

con = pdblp.BCon(debug=False, port=8194, timeout=5000)
con.start()

def import_data():
    
    today = date.today()
    past_date = today - relativedelta(months=+3)
    
    today = date.today().strftime('%Y%m%d')
    past_date = past_date.strftime('%Y%m%d')

    ticker_ls = ['W 1 Comdty','W 2 Comdty','W 3 Comdty','W 4 Comdty', 'W 5 Comdty','W 6 Comdty','C 1 Comdty','C 2 Comdty','C 3 Comdty','C 4 Comdty', 'C 5 Comdty','C 6 Comdty', 'MAZ0UAM1 Index','ARFOCO Index','YC20PRF1 Index', 'MAZ0RS1F Index', 'YCO0PNF1 Index',
        'WHF0UAM1 Index','ARFOWE Index', 'SRS0RSF1 Index','USWTPMN0 Index', 'MAZ0UAM2 Index','ARFOCO1 Index','YC20PRF2 Index','MAZ0RS2F Index','YCO0PNF2 Index', 'WHF0UAM2 Index','ARFOWE1 Index','SRS0RSF2 Index','USWTPMN1 Index',
        'BLY0UAM1 Index','FBARM1 Index','BLY0RSF1 Index', 'BLY0UAM2 Index','FBARM2 Index', 'BLY0RSF2 Index']

    price_data = con.bdh(ticker_ls, 'PX_LAST', past_date, today, longdata=False)
    price_data['N/A'] = 1
    return(price_data)
    
def correlation_calculation():
    today = date.today()
    past_date = today - relativedelta(months=+3)
    
    today = date.today().strftime('%Y%m%d')
    past_date = past_date.strftime('%Y%m%d')

    tickers = ['W 1 Comdty','C 1 Comdty','S 1 Comdty','RLEYJP Comdty','3']
    corn = con.bdh('C 1 Comdty','PX_LAST', past_date, today, longdata=True)

    wheat = con.bdh('W 1 Comdty','PX_LAST', past_date, today, longdata=True)
    barley = con.bdh('RLEYJP Comdty','PX_LAST', past_date, today, longdata=True)
    rice = con.bdh('RR1 Comdty','PX_LAST', past_date, today, longdata=True)
    rapeseed = con.bdh('IJ1 Comdty','PX_LAST', past_date, today, longdata=True)
    sunflower = con.bdh('SU1 Comdty','PX_LAST', past_date, today, longdata=True)
    soybean = con.bdh('S 1 Comdty','PX_LAST', past_date, today, longdata=True)

    temp = [wheat['value'].corr(soybean['value']), wheat['value'].corr(rice['value']),wheat['value'].corr(sunflower['value']),wheat['value'].corr(rapeseed['value']),wheat['value'].corr(barley['value']), barley['value'].corr(soybean['value']),barley['value'].corr(rice['value']),barley['value'].corr(sunflower['value']),barley['value'].corr(rapeseed['value']),barley['value'].corr(wheat['value']),corn['value'].corr(soybean['value']),corn['value'].corr(rice['value']),corn['value'].corr(sunflower['value']),corn['value'].corr(rapeseed['value']),corn['value'].corr(wheat['value']),corn['value'].corr(barley['value'])]
    labels = ['Soybean','Rice','Sunflowerseed','Rapeseed']
    data = [['Labels',labels[0],labels[1],labels[2],labels[3]],['Wheat',wheat['value'].corr(soybean['value']), wheat['value'].corr(rice['value']),wheat['value'].corr(sunflower['value']),wheat['value'].corr(rapeseed['value'])],['Barley',barley['value'].corr(soybean['value']),barley['value'].corr(rice['value']),barley['value'].corr(sunflower['value']),barley['value'].corr(rapeseed['value'])],['Corn',corn['value'].corr(soybean['value']),corn['value'].corr(rice['value']),corn['value'].corr(sunflower['value']),corn['value'].corr(rapeseed['value'])]]
    data = pd.DataFrame(data).transpose()
    new_header = data.loc[0]
    data = data[1:]
    data.columns = new_header
    return data


    
    
def main():
    daily_data= import_data()
    wheat_futures_labels = ['W 1 Comdty','W 2 Comdty','W 3 Comdty','W 4 Comdty', 'W 5 Comdty','W 6 Comdty']
    wheat_futures_contracts = [con.ref(wheat_futures_labels[0], 'FUT_MONTH_YR').value[0],con.ref(wheat_futures_labels[1], 'FUT_MONTH_YR').value[0],con.ref(wheat_futures_labels[2], 'FUT_MONTH_YR').value[0],con.ref(wheat_futures_labels[3], 'FUT_MONTH_YR').value[0],con.ref(wheat_futures_labels[4], 'FUT_MONTH_YR').value[0],con.ref(wheat_futures_labels[5], 'FUT_MONTH_YR').value[0]]
    wheat_futures_prices = daily_data[['W 1 Comdty','W 2 Comdty','W 3 Comdty','W 4 Comdty', 'W 5 Comdty','W 6 Comdty']].iloc[0]
    corn_futures_labels = ['C 1 Comdty','C 2 Comdty','C 3 Comdty','C 4 Comdty', 'C 5 Comdty','C 6 Comdty']
    corn_futures_prices = daily_data[['C 1 Comdty','C 2 Comdty','C 3 Comdty','C 4 Comdty', 'C 5 Comdty','C 6 Comdty']].iloc[0]
    MTD_wheat = [con.ref(wheat_futures_labels[0], 'CHG_PCT_MTD').value[0]/100,con.ref(wheat_futures_labels[1], 'CHG_PCT_MTD').value[0]/100,con.ref(wheat_futures_labels[2], 'CHG_PCT_MTD').value[0]/100,con.ref(wheat_futures_labels[3], 'CHG_PCT_MTD').value[0]/100,con.ref(wheat_futures_labels[4], 'CHG_PCT_MTD').value[0]/100,con.ref(wheat_futures_labels[5], 'CHG_PCT_MTD').value[0]/100]
    
    MTD_corn = [con.ref(corn_futures_labels[0], 'CHG_PCT_MTD').value[0]/100,con.ref(corn_futures_labels[1], 'CHG_PCT_MTD').value[0]/100,con.ref(corn_futures_labels[2], 'CHG_PCT_MTD').value[0]/100,con.ref(corn_futures_labels[3], 'CHG_PCT_MTD').value[0]/100,con.ref(corn_futures_labels[4], 'CHG_PCT_MTD').value[0]/100,con.ref(corn_futures_labels[5], 'CHG_PCT_MTD').value[0]/100]
    
    wheat_futures_prices = wheat_futures_prices.transpose().values.tolist()
    corn_futures_prices = corn_futures_prices.transpose().values.tolist()
    
    df_final_futures = pd.DataFrame({
            'Contract': wheat_futures_contracts, 
            'Wheat CBOT': wheat_futures_prices, 
            'Wheat MTD Change': MTD_wheat,
            'Corn CBOT': corn_futures_prices,
            'Corn MTD Change': MTD_corn,
            })
    
    origin_countries = ['Ukraine','Argentina','Brazil','Russia','US']
    corn_spot = daily_data[['MAZ0UAM1 Index','ARFOCO Index','YC20PRF1 Index', 'MAZ0RS1F Index', 'YCO0PNF1 Index']]
    corn_spot.columns = origin_countries
    corn_spot = [corn_spot['Ukraine'].dropna().iloc[0], corn_spot['Argentina'].dropna().iloc[0], corn_spot['Brazil'].dropna().iloc[0], corn_spot['Russia'].dropna().iloc[0], corn_spot['US'].dropna().iloc[0]]
    corn_spot = pd.DataFrame(corn_spot).transpose()
    corn_spot.columns = origin_countries
    #return(corn_spot)
    corn_forward = daily_data[['MAZ0UAM2 Index','ARFOCO1 Index','YC20PRF2 Index','MAZ0RS2F Index','YCO0PNF2 Index']]
    corn_forward.columns = origin_countries
    corn_forward = [corn_forward['Ukraine'].dropna().iloc[0], corn_forward['Argentina'].dropna().iloc[0], corn_forward['Brazil'].dropna().iloc[0], corn_forward['Russia'].dropna().iloc[0], corn_forward['US'].dropna().iloc[0]]
    corn_forward = pd.DataFrame(corn_forward).transpose()
    corn_forward.columns = origin_countries

    corn_carry = (corn_forward.div(corn_spot)) - 1
    corn_carry = corn_carry.values.tolist()
    
    wheat_spot = daily_data[['WHF0UAM1 Index','ARFOWE Index', 'N/A','SRS0RSF1 Index','USWTPMN0 Index']]
    #wheat_spot = wheat_spot.loc[~wheat_spot.isnull().sum(1).astype(bool)].iloc[0]
    wheat_spot.columns = origin_countries
    wheat_spot = [wheat_spot['Ukraine'].dropna().iloc[0], wheat_spot['Argentina'].dropna().iloc[0], wheat_spot['Brazil'].dropna().iloc[0], wheat_spot['Russia'].dropna().iloc[0], wheat_spot['US'].dropna().iloc[0]]
    #wheat_spot = wheat_spot.dropna().iloc[0]
    wheat_spot = pd.DataFrame(wheat_spot).transpose()
    wheat_spot.columns = origin_countries
    
    wheat_forward = daily_data[['WHF0UAM2 Index','ARFOWE1 Index', 'N/A','SRS0RSF2 Index','USWTPMN1 Index']]
    #wheat_forward = wheat_forward.loc[~wheat_forward.isnull().sum(1).astype(bool)]
    wheat_forward.columns = origin_countries
    wheat_forward = [wheat_forward['Ukraine'].dropna().iloc[0], wheat_forward['Argentina'].dropna().iloc[0], wheat_forward['Brazil'].dropna().iloc[0], wheat_forward['Russia'].dropna().iloc[0], wheat_forward['US'].dropna().iloc[0]]
    wheat_forward = pd.DataFrame(wheat_forward).transpose()
    wheat_forward.columns = origin_countries
    #return(wheat_forward)
    wheat_carry = (wheat_forward / wheat_spot) - 1
    wheat_carry = wheat_carry.values.tolist()
    
    barley_spot = daily_data[['BLY0UAM1 Index','FBARM1 Index', 'N/A', 'BLY0RSF1 Index','N/A']]
    #barley_spot = barley_spot.loc[~barley_spot.isnull.sum(1).astype(bool)].iloc[0]
    barley_spot.columns = origin_countries
    barley_spot = [barley_spot['Ukraine'].dropna().iloc[0], barley_spot['Argentina'].dropna().iloc[0], barley_spot['Brazil'].dropna().iloc[0], barley_spot['Russia'].dropna().iloc[0], barley_spot['US'].dropna().iloc[0]]
    barley_spot = pd.DataFrame(barley_spot).transpose()
    barley_spot.columns = origin_countries
    
    barley_forward = daily_data[['BLY0UAM2 Index','FBARM2 Index', 'N/A', 'BLY0RSF2 Index','N/A']]
   # barley_forward = barley_forward.loc[~barley_forward.isnull.sum(1).astype(bool)].iloc[0]
    barley_forward.columns = origin_countries
    barley_forward = [barley_forward['Ukraine'].dropna().iloc[0], barley_forward['Argentina'].dropna().iloc[0], barley_forward['Brazil'].dropna().iloc[0], barley_forward['Russia'].dropna().iloc[0], barley_forward['US'].dropna().iloc[0]]
    barley_forward = pd.DataFrame(barley_forward).transpose()
    barley_forward.columns = origin_countries
    #return(wheat_forward)
    barley_carry = (barley_forward / barley_spot) - 1
    barley_carry = barley_carry.values.tolist()
    df_final_carry = pd.DataFrame({
           
           'Origin': origin_countries,
           'Wheat': wheat_carry[0],
           'Corn': corn_carry[0],
           'Barley': barley_carry[0]
           
           })

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()

    sql = '''use collateral_prices'''
    cursor.execute(sql)

    for rows, items in df_final_futures.iterrows():
        sql = """INSERT INTO cereal_grains_futures_contracts(Contract, Wheat_CBOT, WHEAT_MTD_Chg, Corn_CBOT, Corn_MTD_Change) values ('%s', '%f', '%f', '%f', '%f')""" %(items['Contract'], items['Wheat CBOT'], items['Wheat MTD Change'], items['Corn CBOT'], items['Corn MTD Change'])
    
    for rows, items in df_final_carry.iterrows():
        sql = """INSERT INTO cereal_grains_carry_calc(Origin, Wheat, Corn, Barley) values ('%s', '%f', '%f', '%f')""" %(items['Origin'], items['Wheat'], items['Corn'], items['Barley'])

    correlation_df = correlation_calculation()

    for rows, items in correlation_df.iterrows():
        sql = """INSERT INTO cereal_grains_correlation_calc(labels, Wheat, Barley, Corn) values ('%s', '%f', '%f', '%f')""" %(items['Labels'], items['Wheat'], items['Barley'], items['Corn'])

    con.stop()
    return (df_final_futures)

x = main()
    