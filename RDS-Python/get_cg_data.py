import pandas as pd
import json
import pymysql
import time

from datetime import datetime
from pycoingecko import CoinGeckoAPI

def get_coin_list():

    coin_ls = [
        'bitcoin',
        'ethereum',
        'binancecoin',
        'binance-peg-cardano',
        'solana',
        'binance-peg-xrp',
        'avalanche-2',
        'chainlink',
        'internet-computer',
        'binance-peg-polkadot',
        'cosmos',
        'monero',
        'near',
        'algorand',
        'stellar',
        'vechain',
        'tezos',
        'helium',
        'flow',
        'kusama',
        'dash',
        'kadena',
        'syscoin',
        'nervos-network',
        'digibyte',
        'celo',
        'unicorn-token',
        'matic-network',
        'cake',
        'sushi',
        'booster',
        'wrapped-bitcoin',
        'wbnb',
        'huobi-btc',
        'honey',
        'crypto-com-chain',
        'ftx-token',
        'kucoin-shares',
        'huobi-token',
        'gatechain-token',
        'okb',
        'binancecoin',
        'axie-infinity',
        'decentraland',
        'san-diego-coin',
        'theta-token',
        'enjincoin',
        'livepeer',
        'ceek',
        'illuvium',
        'victoria-vr',
        'boson-protocol',
        'yield-guild-games',
        'ufo',
        'bora',
        'reef-finance',
        'fantom',
        'aave',
        'maker',
        'oasis-network',
        'loopring',
        'yearn-finance',
        'bancor',
        'ethos',
        'perpetual-protocol',
        '0x',
        'havven',
        'serum',
        'request-network',
        'dydx',
        'origin-protocol',
        'terra-luna',
        '1inch',
        'curve-dao-token',
        'harmony',
        'binance-peg-eos',
        'golden-ratio-token',
        'secret',
        'zilliqa',
        'waves',
        'bitclout',
        'tether',
        'usd-coin',
        'binance-usd',
        'terrausd',
        'true-usd',
        'paxos-standard',
        'neutrino',
        'fei-usd',
        'gemini-dollar',
        'limited-usd',
        'husd',
        'origin-dollar',
        'binance-peg-filecoin',
        'iota',
        'amp-token',
        'chiliz',
        'ankr',
        'immutable-x',
        'audius',
        'ocean-protocol',
        'energy-web-token',
        'digitalbits',
        'constellation-labs',
        'centrifuge',
        'binance-peg-dogecoin',
        'shiba-inu',
        'dogelon-mars',
        'unibright',
        'game',
        'balancer',
        'binance-peg-litecoin',
        'raydium',
        'iexec-rlc',
        'kyber-network-crystal',
    ]

    return coin_ls

def get_historical_data(coin):

    cg = CoinGeckoAPI()
    data = cg.get_coin_market_chart_range_by_id(id = coin, vs_currency = 'usd', from_timestamp = '1644220780', to_timestamp = '1644335980')
    
    return data

def upload_to_AWS(data_df, name, description, cursor, db):

    value_column = data_df.columns.values[0]

    for i in range(0, data_df.shape[0]):

        sql = ''' insert into cg_data (Name, ValueDesc, Value, Date) values ('%s', '%s', '%g', '%s') ''' % (name, description, data_df[value_column].iloc[i], data_df.index[i])
        cursor.execute(sql)
        db.commit()


def main():
    
    db = pymysql.connect(host = 'database-1.c8dbzf9wtrjo.us-east-2.rds.amazonaws.com', user = 'admin', password = 'Ktr321ugh!')
    cursor = db.cursor()

    sql = '''
        use c_price_sheet 
    '''
    cursor.execute(sql)
    cursor.connection.commit()


    x = get_coin_list()

    for i in range(0,len(x)):
        while True:
            try:
                print('i: ' + str(i))
                print('Coin: ' + str(x[i]))
                data = get_historical_data(x[i])
                prices = pd.DataFrame(data['prices'], columns = ['Date', 'Price'])
                prices['Date'] = pd.to_datetime(prices['Date'], unit = 'ms')
                prices.index = prices['Date']
                prices = prices.drop(['Date'], axis = 1)
                market_caps = pd.DataFrame(data['market_caps'], columns = ['Date','Total Market Cap'])
                market_caps['Date'] = pd.to_datetime(market_caps['Date'], unit = 'ms')
                market_caps.index = market_caps['Date']
                market_caps = market_caps.drop(['Date'], axis = 1)
                total_volumes = pd.DataFrame(data['total_volumes'], columns = ['Date','Total Volume'])
                total_volumes['Date'] = pd.to_datetime(total_volumes['Date'], unit = 'ms')
                total_volumes.index = total_volumes['Date']
                total_volumes = total_volumes.drop(['Date'], axis = 1)
                upload_to_AWS(prices, x[i], description='Price (USD)', cursor = cursor, db = db)
                upload_to_AWS(market_caps, x[i], description='Market Cap (USD)', cursor = cursor, db = db)
                upload_to_AWS(total_volumes, x[i], description='Total Volume', cursor = cursor, db = db)
            except:
                print('Sleeping')
                time.sleep(60)
                continue
            break

    sql = '''SELECT * from cg_data'''
    cursor.execute(sql)
    print(cursor.fetchall())
    db.close()
    return prices, market_caps, total_volumes



print(main())