#DB name: Alma_DB
#DB instance identifier: database-1
#username: admin
#pass: Ktr321ugh!
#host: database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com
#port:3306


import pymysql 
import pandas as pd
import numpy as np
import datetime
import snscrape.modules.twitter as sntwitter
import pandas as pd
import numpy as np
from textblob import TextBlob
import re
import matplotlib.pyplot as plt


import warnings
warnings.filterwarnings('ignore')

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

def main3():

    """
    daily_data_df = pd.read_csv('C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\Daily_Update_test.csv')
    currency_base_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\Currency_Table.xlsx", sheet_name='Base Currency')
    currency_exchange_ticker_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\Currency_Table.xlsx", sheet_name='Conversion')
    conversion_ratios_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\220426_ConversionRatios.xlsx")
    daily_data_df['Date'] = pd.to_datetime(daily_data_df['Date'])

    temp = pd.DataFrame(daily_data_df['Ticker'], columns = ['Ticker'])
    temp_ls = np.zeros(temp.shape[0])
    temp['Base Currency'] = temp_ls
    temp['FX Conversion Ticker'] = temp_ls
    ticker_unique_df = temp.drop_duplicates()

    for i in range(0, ticker_unique_df.shape[0]):
        temp_mask = temp.mask(temp['Ticker'] == (ticker_unique_df.iat[i,0]), True)
        temp_index = temp_mask.loc[temp_mask['Ticker'] == True].reset_index()
        temp_index = temp_index['index'].values
        base_currency = currency_base_df['Base Currency'].loc[currency_base_df['Ticker'] == ticker_unique_df.iat[i,0]].values[0]
        try:
            fx_ticker = currency_exchange_ticker_df['Conversion to USD Ticker'].loc[currency_exchange_ticker_df['Base Currency'] == base_currency].values[0]
        except:
            fx_ticker = 'N/A'

        if base_currency == 'USD':
            fx_ticker = 'N/A'
        
        if base_currency == '':
            base_currency = 'N/A'

        for q in range(len(temp_index)):
            temp['Base Currency'].iat[temp_index[q]] = base_currency
            temp['FX Conversion Ticker'].iat[temp_index[q]] = fx_ticker

    daily_data_df['Base Currency'] = temp['Base Currency']
    daily_data_df['FX Conversion Ticker'] = temp['FX Conversion Ticker']

    temp_converted = []

    for index, rows in daily_data_df.iterrows():

        temp_units = rows['Units']

        temp_conv_method = conversion_ratios_df['Conversion Method'].loc[conversion_ratios_df['Units'] == temp_units].values[0]

        print(rows['Value'])
        #print(temp_conv_method)

        if temp_conv_method == ('NaN'):
            print('Here')
            temp_converted.append(rows['Value'])
        
        elif temp_conv_method == ('Units'):
            rows['Value'] = float(rows['Value'])
            temp_conv_ratio = conversion_ratios_df['Factor'].loc[conversion_ratios_df['Units'] == temp_units].values[0]
            temp_converted.append(rows['Value'] * float(temp_conv_ratio))

        elif temp_conv_method == ('FX'):
            rows['Value'] = float(rows['Value'])
            fx_conversion_ticker = rows['FX Conversion Ticker']
            temp_fx_df = daily_data_df.loc[daily_data_df['Ticker'] == fx_conversion_ticker]
            try:
                temp_fx_date = temp_fx_df['Date'].loc[temp_fx_df['Date'] == temp_fx_df['Date'].max()].values[0]
            except:
                print('FX Error')
                print(fx_conversion_ticker)
                print(rows['Description'])
                temp_converted.append(0)
                continue
                
            fx_conversion_ratio = float(temp_fx_df['Value'].loc[temp_fx_df['Date'] == temp_fx_date].values[0])
            print(fx_conversion_ratio)
            if (fx_conversion_ticker == 'GBP Curncy' or fx_conversion_ticker == 'EUR Curncy'):
                temp_converted.append(rows['Value'] * fx_conversion_ratio)
            else:
                temp_converted.append(rows['Value'] / fx_conversion_ratio)

        elif temp_conv_method == ('FX & Units'):
            rows['Value'] = float(rows['Value'])
            temp_conv_ratio = conversion_ratios_df['Factor'].loc[conversion_ratios_df['Units'] == temp_units].values[0]
            temp_converted_val = (rows['Value'] * temp_conv_ratio)
            fx_conversion_ticker = rows['FX Conversion Ticker']
            temp_fx_df = daily_data_df.loc[daily_data_df['Ticker'] == fx_conversion_ticker]
            temp_fx_date = temp_fx_df['Date'].loc[temp_fx_df['Date'] == temp_fx_df['Date'].max()].values[0]
            fx_conversion_ratio = float(temp_fx_df['Value'].loc[temp_fx_df['Date'] == temp_fx_date].values[0])
            if (fx_conversion_ticker == 'GBP Curncy' or fx_conversion_ticker == 'EUR Curncy'):
                temp_converted.append(temp_converted_val * fx_conversion_ratio )
            else:
                temp_converted.append(temp_converted_val / fx_conversion_ratio)

        elif temp_conv_method == ('Units (bu)'):

            rows['Value'] = float(rows['Value'])

            if 'Soybean' in rows['Description']:
                bu_conv_ratio = conversion_ratios_df['Soybean Factor'].loc[conversion_ratios_df['Units'] == temp_units]
            elif 'Corn' in rows['Description']:
                bu_conv_ratio = conversion_ratios_df['Corn Factor'].loc[conversion_ratios_df['Units'] == temp_units]
            elif 'Wheat' in rows['Description']:
                bu_conv_ratio = conversion_ratios_df['Wheat Factor'].loc[conversion_ratios_df['Units'] == temp_units]
            elif 'Sorghum' in rows['Description']:
                bu_conv_ratio = conversion_ratios_df['Sorghum Factor'].loc[conversion_ratios_df['Units'] == temp_units]

            temp_converted.append(rows['Value'] * float(bu_conv_ratio))

        else:

            temp_converted.append(rows['Value'])

    daily_data_df['Value Adjusted'] = temp_converted
    
    daily_data_df.to_excel('C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\Test_Table_220426.xlsx')
    """
    

    daily_data_df = pd.read_excel('C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\Test_Table_220426.xlsx')
    
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    
    sql = '''use collateral_prices'''
    cursor.execute(sql)

    sql = '''drop table c_price_historical'''
    cursor.execute(sql)
    
    
    sql = '''create table c_price_historical(
        Id int auto_increment not null,
        ticker text(255) ,
        description text(255) ,
        origin text(255) ,
        dashboard text(255) ,
        units text(255) ,
        update_frequency text(255) ,
        value float ,
        date Datetime ,
        base_currency text(255) , 
        fx_conversion_ticker text(255) ,
        value_adjusted float , 
        PRIMARY KEY (id)
        )'''

    cursor.execute(sql)
    
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())
    
    daily_data_df = daily_data_df.loc[daily_data_df['Ticker'] != 'MZN Curncy']
    
    for rows in daily_data_df['Value Adjusted']:
        if type(rows) == 'str':
            print(rows)
    print(daily_data_df['Date'])
    daily_data_df['Value'] = pd.to_numeric(daily_data_df['Value'], downcast = 'float')
    daily_data_df['Date'] == pd.to_datetime(daily_data_df['Date'], format = "%Y-%m-%d")
    print(daily_data_df['Date'])
    for i in range(0, daily_data_df.shape[0]):
        print(i)
        ticker_temp = daily_data_df['Ticker'].iat[i]
        desc_temp = daily_data_df['Description'].iat[i]
        origin_temp = daily_data_df['Origin'].iat[i]
        dash_temp = daily_data_df['Dashboard'].iat[i]
        unit_temp = daily_data_df['Units'].iat[i]
        update_temp = daily_data_df['Update Frequency'].iat[i]
        value_temp = daily_data_df['Value'].iat[i]
        date_temp = str(daily_data_df['Date'].iat[i])
        print(date_temp)
        value_adjusted_temp = daily_data_df['Value Adjusted'].iat[i]
        #date_temp = datetime.datetime.fromtimestamp(date_temp)
        #date_temp = date_temp.strftime("%d/%m/%Y")
        base_currency_temp = daily_data_df['Base Currency'].iat[i]
        fx_conversion_ticker = daily_data_df['FX Conversion Ticker'].iat[i]

        #print(str(type(ticker_temp)) + ' ' + str(type(desc_temp)) + ' ' + str(type(origin_temp)) + ' ' + str(type(dash_temp)) + ' ' + str(type(unit_temp)) + ' ' + str(type(update_temp)) + ' ' + str(type(value_temp)) +' ' + str(type(date_temp)))

        sql = '''insert into c_price_historical (ticker, description, origin, dashboard, units, update_frequency, value, date, base_currency, fx_conversion_ticker, value_adjusted) values ('%s' , '%s', '%s' , '%s' , '%s', '%s', '%g', '%s', '%s', '%s', '%g')''' % (ticker_temp, desc_temp, origin_temp, dash_temp, unit_temp, update_temp, value_temp, date_temp, base_currency_temp, fx_conversion_ticker, float(value_adjusted_temp))
        cursor.execute(sql)
        db.commit()
    
    
    sql = '''SELECT * from c_price_historical'''
    cursor.execute(sql)
    print(cursor.fetchall())
    
    db.close()

def main():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    #print(cursor.execute("select version()"))
    #data = cursor.fetchone()
    #print(data)
    #data_df = pd.read_csv("C:\\Users\\ChristopherTHOMPSON\\Documents\\C_Price_Sheet_Map.csv")
    
    select_database(db, cursor, 't')
    cursor.connection.commit()

    sql = '''show tables'''
    cursor.execute(sql)
    print(cursor.fetchall())
    """
    for i in range(0,data_df.shape[0]):

        temp_ls = data_df.iloc[i].values
        sql = '''insert into c_price_sheet_map (CMC_Name, CMC_Symbol, Sector_of_Focus, Blockchain_Used, CG_Name) values ('%s', '%s', '%s', '%s', '%s')''' % (temp_ls[0], temp_ls[1], temp_ls[2], temp_ls[3], temp_ls[4])
        cursor.execute(sql)
        db.commit()
    """
    sql = '''SELECT * from c_price_sheet_map'''
    cursor.execute(sql)
    print(cursor.fetchall())
    db.close()

    return 0

    sql = '''
        CREATE TABLE c_price_sheet_map (
            Id int auto_increment not null,
            CMC_Name text(255) ,
            CMC_Symbol text(255) ,
            Sector_of_Focus text(255) ,
            Blockchain_Used text(255) ,
            CG_Name text(255) ,
            PRIMARY KEY (Id)
            )
    '''

    cursor.execute(sql)
    
    sql = '''show tables'''
    
    print(cursor.execute(sql))

    return 0

    """
    sql = 'SELECT * FROM cmc_map_2'
    cursor.execute(sql)
    print(cursor.fetchall())

    """
    """
    sql = '''
    insert into cmc_map_2(fname, lname) values('%s', '%s')''' % ('chris','thompson')
    #cursor.execute(sql)
    #db.commit()

    sql = ''' select * from person'''
    cursor.execute(sql)
    print(cursor.fetchall())
    """
    
    db.close()

def main_2():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    cursor.connection.commit()
    """
    sql = '''create table cmc_data_final_2 (
            Id int auto_increment not null,
            cmc_name text(255) ,
            symbol text(255) ,
            market_cap float ,
            market_cap_strict float ,
            date_added text(255) ,
            price float ,
            circulating_supply float ,
            total_supply float ,
            max_supply float ,
            num_market_pairs float ,
            volume_24hr float ,
            volume_24hr_chg float ,
            pct_change_1hr float ,
            pct_change_24hr float ,
            pct_change_7day float ,
            PRIMARY KEY (Id)
            )
        '''

    cursor.execute(sql)
    """

    data_df = grab_cmc_data.main()

    for key in data_df:
        temp = data_df[key]

        temp2 = temp['quote']
        temp2 = temp2['USD']
        print(temp)
        print(temp2)
        print(str(temp['name']), str(temp['symbol']), temp2['market_cap'], temp2['fully_diluted_market_cap'], temp['date_added'], temp2['price'], temp['circulating_supply'], temp['total_supply'], temp['max_supply'], temp['num_market_pairs'], temp2['volume_24h'], temp2['volume_change_24h'], temp2['percent_change_1h'], temp2['percent_change_24h'], temp2['percent_change_7d'])
        """
        if temp['max_supply']:
            max_sply = 0
        else:
            max_sply = temp['max_supply']
        """
        max_sply = 0
        sql = '''insert into cmc_data_final_2(cmc_name, symbol, market_cap, market_cap_strict, date_added, price, circulating_supply, total_supply, max_supply, num_market_pairs, volume_24hr, volume_24hr_chg, pct_change_1hr, pct_change_24hr, pct_change_7day) values('%s', '%s', '%g', '%g', '%s', '%g', '%g', '%g', '%g', '%g', '%g', '%g', '%g', '%g', '%g')''' % (temp['name'], temp['symbol'], float(temp2['market_cap']), float(temp2['fully_diluted_market_cap']), str(temp['date_added']), temp2['price'], temp['circulating_supply'], temp['total_supply'], max_sply, temp['num_market_pairs'], temp2['volume_24h'], temp2['volume_change_24h'], temp2['percent_change_1h'], temp2['percent_change_24h'], temp2['percent_change_7d'])
        cursor.execute(sql)
        db.commit()

    sql = '''show tables'''
    cursor.execute(sql)
    print(cursor.fetchall())

    db.close()

def main4():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    cursor.connection.commit()

    sql = '''use collateral_prices'''
    cursor.execute(sql)

    sql = '''SELECT * from c_price_historical'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

    sql = '''drop table c_price_historical'''
    cursor.execute(sql)
    sql = '''SELECT * from c_price_historical'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

def main5():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    cursor.connection.commit()

    sql = '''drop table risk_advisory_financial_performance'''
    cursor.execute(sql)

    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\Data_Aggregation_for_Database.xlsx", sheet_name = 'Financial Performance')

    
    sql = '''create table risk_advisory_financial_performance(
                Id int auto_increment not null,
                Variable_name_eng text(255) ,
                Variable_name_port text(255) ,
                Data_group text(255) ,
                Units text(255) ,
                Season text(255) ,
                Date_published datetime ,
                Value float ,
                Most_recent_company_forecast int ,
                PRIMARY KEY (Id)
                )
                '''
    

    cursor.execute(sql)
    

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    
    for index, row in data_df.iterrows():

        sql = '''insert into risk_advisory_financial_performance(Variable_name_eng, Variable_name_port, Data_group, Units, Season, Date_published, Value, Most_recent_company_forecast) values('%s', '%s', '%s', '%s', '%s', '%s', '%g', '%g')''' % (row['English Name'], row['Portuguese Name'], row['Data Group'], row['Units'], row['Season'], row['Date Published'].strftime("%Y-%m-%d"), row['Value'], row['Most Recent Company Forecast'])
        cursor.execute(sql)
        db.commit()        
    
    
    sql = '''SELECT * from risk_advisory_financial_performance'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

def main6():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')

    sql = '''drop table ukr_price_test'''
    cursor.execute(sql)
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\ukr_prices_test.xlsx", sheet_name = 'Sheet3')

    temp = data_df['date']

    print(temp)

    for i in range(0, temp.shape[0]):

        print(temp.iat[i].strftime("%Y-%m-%d"))
    
    sql = '''create table ukr_price_test(
                Id int auto_increment not null,
                date datetime ,
                ticker text(255) ,
                field text(255) ,
                value float ,
                units text(255) ,
                description text ,
                PRIMARY KEY (Id)
                )
                '''
    

    cursor.execute(sql)
    

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    
    for index, row in data_df.iterrows():

        sql = '''insert into ukr_price_test(date, ticker, field, value, units, description) values('%s', '%s', '%s', '%g', '%s', '%s')''' % (row['date'].strftime("%Y-%m-%d"), row['ticker'], row['field'], row['value'], row['units'], row['description'])
        cursor.execute(sql)
        db.commit()        
    
    
    sql = '''SELECT * from ukr_price_test'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

def main7():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')

    #sql = '''drop table mc_results_weekly'''
    #cursor.execute(sql)
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    
    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\Visual_Code_Workplace\\MC_Code\\mc_results_return_2.xlsx")

    """
    sql = '''create table mc_results_weekly(
                Id int auto_increment not null,
                simulation_date datetime ,
                date datetime ,
                variable text(255) ,
                value float ,
                PRIMARY KEY (Id)
                )
                '''
    

    cursor.execute(sql)
    """

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    
    for index, row in data_df.iterrows():

        sim_date_temp = datetime.datetime.strptime(row['Simulation Date'], "%d/%m/%Y")
        date_temp = datetime.datetime.strptime(row['Date'], "%d/%m/%Y")

        sql = '''insert into mc_results_weekly(simulation_date, date, variable, value) values('%s', '%s', '%s', '%g')''' % (sim_date_temp.strftime("%Y-%m-%d"), date_temp.strftime("%Y-%m-%d"), row['variable'], row['value'])
        cursor.execute(sql)
        db.commit()        
    
    
    
    sql = '''SELECT * from mc_results_weekly'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))
    
def main8():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\MC_Code\\volatilities_mc_results.xlsx")

    sql = '''create table mc_volatilities_weekly(
                Id int auto_increment not null,
                simulation_date datetime ,
                variable text(255) ,
                value float ,
                PRIMARY KEY (Id)
                )
                '''
    

    #cursor.execute(sql)
    

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    """
    for index, row in data_df.iterrows():

        sim_date_temp = datetime.datetime.strptime(row['Simulation Date'], "%d/%m/%Y")

        sql = '''insert into mc_volatilities_weekly(simulation_date, variable, value) values('%s', '%s', '%g')''' % (sim_date_temp.strftime("%Y-%m-%d"), row['variable'], row['value'])
        cursor.execute(sql)
        db.commit()        
    """
    
    
    sql = '''SELECT * from mc_volatilities_weekly'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

def main9():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    sql = '''drop table market_data_sugar_advisory'''
    cursor.execute(sql)

    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\Market_Data_Test.xlsx")

    sql = '''create table market_data_sugar_advisory(
                Id int auto_increment not null,
                date datetime ,
                variable text(255) ,
                value float ,
                description text(255) ,
                units text(255) ,
                PRIMARY KEY (Id)
                )
                '''
    

    #cursor.execute(sql)
    

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())


    for index, row in data_df.iterrows():

        #sim_date_temp = datetime.datetime.strptime(row['date'], "%d/%m/%Y")

        sql = '''insert into market_data_sugar_advisory(date, variable, value, description, units) values('%s', '%s', '%g', '%s', '%s')''' % (row['date'].strftime("%Y-%m-%d"), row['ticker'], row['value'], row['description'], row['units'])
        cursor.execute(sql)
        db.commit()        
    
    
    sql = '''SELECT * from market_data_sugar_advisory'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

def main10():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())
    
    """
    sql = '''drop table market_forecasts_sugar_advisory'''
    cursor.execute(sql)
    
    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\test_final_agg_output.xlsx")
    data_df['Forecast Date'] = pd.to_datetime(data_df['Forecast Date'], format = "%d/%m/%Y")
    data_df['Simulation Date'] = pd.to_datetime(data_df['Simulation Date'], format = "%d/%m/%Y")

    """
    sql = '''create table market_forecasts_sugar_advisory(
                Id int auto_increment not null,
                Date_published datetime ,
                Variable text(255) ,
                Mean_returned float ,
                Forecast_date datetime ,
                Std float ,
                PRIMARY KEY (Id)
                )
                '''
    

    cursor.execute(sql)
    

    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\test_final_agg_output_2.xlsx")
    data_df['Forecast Date'] = pd.to_datetime(data_df['Forecast Date'], format = "%Y-%m-%d")
    data_df['Simulation Date'] = pd.to_datetime(data_df['Simulation Date'], format = "%d/%m/%Y")
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    
    for index, row in data_df.iterrows():

        #sim_date_temp = datetime.datetime.strptime(row['date'], "%d/%m/%Y")

        sql = '''insert into market_forecasts_sugar_advisory(Date_published, Variable, Mean_returned, Forecast_date, Std) values('%s', '%s', '%g', '%s', '%g')''' % (row['Simulation Date'].strftime("%Y-%m-%d"), row['Variabe'], row['Avg Return'], row['Forecast Date'].strftime("%Y-%m-%d"), row['STD'])
        cursor.execute(sql)
        db.commit()        
    
    
    
    sql = '''SELECT * from market_forecasts_sugar_advisory'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

def main11():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    select_database(db, cursor, 't')
    
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    #sql = '''drop table source_forecasts_sugar_advisory'''
    #cursor.execute(sql)

    data_df = pd.read_excel("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Visual_Code_Workplace\\RDS-Python\\forecast_data.xlsx", sheet_name = 'Sheet4')


    sql = '''create table source_forecasts_sugar_advisory(
                Id int auto_increment not null,
                Date_published datetime ,
                Variable text(255) ,
                Forecasted_value float ,
                Forecast_period datetime ,
                Description text(255) ,
                Units text(255) ,
                PRIMARY KEY (Id)
                )
            '''
    

    cursor.execute(sql)
    

    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    
    for index, row in data_df.iterrows():

        #sim_date_temp = datetime.datetime.strptime(row['date'], "%d/%m/%Y")

        sql = '''insert into source_forecasts_sugar_advisory(Date_published, Variable, Forecasted_value, Forecast_period, Description, Units) values('%s', '%s', '%g', '%s', '%s', '%s')''' % (row['date'].strftime("%Y-%m-%d"), row['ticker'], row['value'], row['Forecast Period'].strftime("%Y-%m-%d"), row['Description'], row['Units'])
        cursor.execute(sql)
        db.commit()        
    
    
    
    sql = '''SELECT * from source_forecasts_sugar_advisory'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))


def update_market_data():

    new_data_df = pd.read_csv("C:\\Users\\Chris\\Documents\\AlmaStone Workspace\\Market Prices\\Daily_Update_230228.csv", low_memory = False, index_col=False)
    new_data_df['Date'] = pd.to_datetime(new_data_df['Date'])
    new_data_df['Value'] = new_data_df['Value'].astype('float64')
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()

    sql = '''use collateral_prices'''
    cursor.execute(sql)

    columns = ['Id','Ticker', 'Description', 'Origin', 'Dashboard', 'Units', 'Date','Value','Most Recent']
    sql = '''
        WITH list AS (
            SELECT m.*, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM market_data_prices as m
        )
        SELECT * FROM list where rn = 1;
        '''

    data_df = pd.read_sql(sql = sql, con = db)
    
    counter = 0
    length = data_df.shape[0]
    for rows, items in data_df.iterrows():
        print('Number: ' +str(counter))
        print('% Complete: ' + str(int(counter/length*100)) + '%')
        temp_tick = items.ticker
        temp_date = items.date
        new_df = new_data_df.loc[(new_data_df['Ticker'] == temp_tick) & (new_data_df['Date'] > temp_date)]
        columns = ['Ticker', 'Description', 'Origin', 'Dashboard', 'Units', 'Date','Value']
        new_df = new_df[columns]
        for row, items in new_df.iterrows():
            sql = '''INSERT INTO market_data_prices(ticker, description, origin, dashboard, units, date, value) values('%s', '%s', '%s', '%s', '%s', '%s', '%g')''' % (items['Ticker'], items['Description'], items['Origin'], items['Dashboard'], items['Units'], items['Date'],items['Value'])
            try:
                cursor.execute(sql)
                db.commit()
            except:
                db.rollback()
                print('Error')
        counter += 1

def initial_market_data():

    new_data_df = pd.read_csv("C:\\Users\\ChristopherTHOMPSON\\Desktop\\Market Prices\\Daily_Update_new.csv", low_memory = False, index_col=False)
    new_data_df['Date'] = pd.to_datetime(new_data_df['Date'])
    new_data_df['Value'] = new_data_df['Value'].astype('float64')
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()

    sql = '''use collateral_prices'''
    cursor.execute(sql)
    
    """
    sql = '''SELECT * from market_data_prices'''
    cursor.execute(sql)
    print(cursor.fetchall())

    sql = '''
        WITH list AS (
            SELECT m.*, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM market_data_prices as m
        )
        SELECT * FROM list where rn = 1;
        '''
    cursor.execute(sql)
    print(cursor.fetchall())
    return 0
    """
    new_data_df = new_data_df.loc[new_data_df['Ticker'].isin(['BAAWHYDP Index', 'BAAWANAB Index'])]

    counter = 0
    length = new_data_df.shape[0]
    for rows, items in new_data_df.iterrows():
        print(counter)
        print(length)
        print(str(int(counter / length )) + ' % complete')
        print()
        if counter > 0:
            sql = '''INSERT INTO market_data_prices(ticker, description, origin, dashboard, units, date, value) values('%s', '%s', '%s', '%s', '%s', '%s', '%g')''' % (items['Ticker'], items['Description'], items['Origin'], items['Dashboard'], items['Units'], items['Date'],items['Value'])
            cursor.execute(sql)
            db.commit()
        counter += 1
 
    db.close()

def futures_contracts():
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    sql = '''use collateral_prices'''
    cursor.execute(sql)
        
    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())


    sql = '''create table cereal_grains_correlation_calc(
                Id int auto_increment not null,
                labels text(255),
                Wheat float ,
                Barley float ,
                Corn float ,
                PRIMARY KEY (Id)
                )
            '''

    sql = '''create table cereal_grains_futures_contracts(
                Id int auto_increment not null,
                Contract text(255),
                Wheat_CBOT float ,
                Wheat_MTD_Chg float ,
                Corn_CBOT float ,
                Corn_MTD_Change float ,
                PRIMARY KEY (Id)
                )
            '''


    sql = '''create table cereal_grains_carry_calc(
                Id int auto_increment not null,
                Origin text(255),
                Wheat float ,
                Corn float ,
                Barley float ,
                PRIMARY KEY (Id)
                )
                '''



    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    db.close()

#clean the tweets with a function
def cleanTweets(text):
    text = re.sub('@[A-Za-z0-9_]+', '', text) #removes @mentions
    text = re.sub('#','',text) #removes hastag '#' symbol
    text = re.sub('RT[\s]+','',text)
    text = re.sub('https?:\/\/\S+', '', text) 
    text = re.sub('\n',' ',text)
    return text

#get subjectivity and polarity of tweets with a function
def getSubjectivity(text):
    return TextBlob(text).sentiment.subjectivity
#get polarity with a function
def getPolarity(text):
    return TextBlob(text).sentiment.polarity

#create a function to check negative, neutral and positive analysis
def getAnalysis(score):
    if score<0:
        return 'Negative'
    elif score ==0:
        return 'Neutral'
    else:
        return 'Positive'
    

def news_scraper():
   # Create a list to append tweet data
    tweets_list = []
    maxTweets = 10
    # Using TwitterSearchScraper to scrape data and append tweets to list
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper('sugar prices since:2022-11-01 until:{today}').get_items()):
        print(tweet)
        if i>maxTweets:
            break
        tweets_list.append([tweet.content])
        
    # Creating a dataframe from the tweets list above
    tweets_to_df = pd.DataFrame(tweets_list, columns=['Tweets'])

    tweets_to_df['cleanedTweets'] = tweets_to_df['Tweets'].apply(cleanTweets) #apply cleanTweet function to the tweet
    tweets_to_df.head() #compares original tweets with cleaned Tweets

    tweets_to_df.to_csv('tweets_crypto.csv') #write dataframe into csv file
    print(tweets_to_df.head(5))
    savedTweets = pd.read_csv('tweets_crypto.csv',index_col=0) #reads csv file

    savedTweets['Subjectivity'] = savedTweets['cleanedTweets'].apply(getSubjectivity)
    savedTweets['Polarity'] = savedTweets['cleanedTweets'].apply(getPolarity)
    savedTweets.drop('Tweets', axis=1).head() #shows polarity and subjectivity of each tweet and drops the uncleaned tweets column

    savedTweets['Analysis'] = savedTweets['Polarity'].apply(getAnalysis)
    savedTweets['Analysis'].value_counts() #shows the counts of tweets' polarity

    print(savedTweets.head(5))

    #plot a bar graph to show count of tweet sentiment
    fig = plt.figure(figsize=(7,5))
    color = ['green','grey','red']
    savedTweets['Analysis'].value_counts().plot(kind='bar',color = color)
    plt.title('Value count of tweet polarity')
    plt.ylabel('Count')
    plt.xlabel('Polarity')
    plt.grid(False)
    plt.show()
    #pie chart to show percentage distribution of polarity
    fig = plt.figure(figsize=(7,7))
    colors = ('green', 'grey', 'red')
    wp={'linewidth':2, 'edgecolor': 'black'}
    tags=savedTweets['Analysis'].value_counts()
    explode = (0.1,0.1,0.1)
    tags.plot(kind='pie', autopct='%1.1f%%', shadow=True, colors=colors, 
            startangle=90, wedgeprops=wp, explode=explode, label='')
    plt.title('Distribution of polarity')
    #plot the polarity and subjectivity on a scatter plot
    plt.figure(figsize=(9,7))
    for i in range(0,savedTweets.shape[0]):
        plt.scatter(savedTweets['Polarity'][i],savedTweets['Subjectivity'][i], color='blue')
    plt.title('Sentiment Analysis')
    plt.xlabel('Polarity')
    plt.ylabel('Subjectivity')
    plt.show()
    return savedTweets



def create_news_headline_table():
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    sql = '''use collateral_prices'''
    cursor.execute(sql)
    
    sql = '''drop table news_headline_table'''
    #cursor.execute(sql)

    sql = '''create table news_headline_table(
                Id int auto_increment not null,
                commodity text(255),
                headline text(500),
                date_year float,
                date_month float,
                date_day float,
                PRIMARY KEY (Id)
                )
            '''

    cursor.execute(sql)


    sql = '''show tables'''
    print(cursor.execute(sql))
    print(cursor.fetchall())

    db.close()

def add_headline():

    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()
    sql = '''use collateral_prices'''
    cursor.execute(sql)

    data_df = pd.read_excel("230110_headlines.xlsx")

    sql = '''SELECT * from news_headline_table'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)

    for index, row in data_df.iterrows():

        #sim_date_temp = datetime.datetime.strptime(row['date'], "%d/%m/%Y")

        sql = '''insert into news_headline_table(commodity, headline, date_year, date_month, date_day) values('%s', '%s', '%g', '%g', '%g')''' % (row['commodity'], row['headline'], row['date_year'], row['date_month'], row['date_day'])
        cursor.execute(sql)
        db.commit()        
    
    
    
    sql = '''SELECT * from news_headline_table'''
    cursor.execute(sql)
    results = cursor.fetchall()
    print(results)
    print(len(results))

update_market_data()