from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

import pandas as pd
from json import decoder
from coinmarketcapapi import CoinMarketCapAPI, CoinMarketCapAPIError


def get_coin_list():
    ticker_ls = [
        'BTC',
        'ETH',
        'BNB',
        'ADA',
        'SOL',
        'XRP',
        'AVAX',
        'LINK',
        'ICP',
        'DOT',
        'ATOM',
        'XMR',
        'NEAR',
        'ALGO',
        'XLM',
        'VET',
        'XTZ',
        'HNT',
        'FLOW',
        'KSM',
        'DASH',
        'KDA',
        'SYS',
        'CKB',
        'DGB',
        'CELO',
        'UNI',
        'MATIC',
        'CAKE',
        'SUSHI',
        'BOO',
        'WBTC',
        'WBNB',
        'HBTC',
        'HNY',
        'CRO',
        'FTT',
        'KCS',
        'HT',
        'GT',
        'OKB',
        'BNB',
        'AXS',
        'MANA',
        'SAND',
        'THETA',
        'ENJ',
        'LPT',
        'CEEK',
        'ILV',
        'VR',
        'BOSON',
        'YGG',
        'UFO',
        'BORA',
        'REEF',
        'FTM',
        'AAVE',
        'MKR',
        'ROSE',
        'LRC',
        'YFI',
        'BNT',
        'VGX',
        'PERP',
        'ZRX',
        'SNX',
        'SRM',
        'REQ',
        'DYDX',
        'OGN',
        'LUNA',
        '1INCH',
        'CRV',
        'ONE',
        'EOS',
        'GRT',
        'SCRT',
        'ZIL',
        'WAVES',
        'DESO',
        'USDT',
        'USDC',
        'BUSD',
        'UST',
        'TUSD',
        'USDP',
        'USDN',
        'FEI',
        'GUSD',
        'LUSD',
        'HUSD',
        'OUSD',
        'FIL',
        'MIOTA',
        'AMP',
        'CHZ',
        'ANKR',
        'IMX',
        'AUDIO',
        'OCEAN',
        'EWT',
        'XDB',
        'DAG',
        'CFG',
        'DOGE',
        'SHIB',
        'ELON',
        'UBT',
        'GTC',
        'BAL',
        'LTC',
        'RAY',
        'RLC',
        'KNC'
    ]

    name_ls = [

        'Bitcoin',
        'Ethereum',
        'Binance',
        'Cardano',
        'Solana',
        'Ripple',
        'Avalanche',
        'ChainLink',
        'Internet Computer',
        'Polkadot',
        'Cosmos',
        'Monero',
        'NEAR Protocol',
        'Algorand',
        'Stellar',
        'VeChain',
        'Tezos',
        'Helium',
        'Flow',
        'Kusama',
        'Dash',
        'Kadena',
        'Syscoin',
        'Nervos Network',
        'DigiByte',
        'Celo',
        'UniSwap',
        'Polygon',
        'Pancakeswap',
        'SushiSwap',
        'SpookySwap',
        'Wrapped Bitcoin',
        'Wrapped BNB',
        'Huobi HBTC',
        'Honeyswap',
        'Crypto.com',
        'FTX Token',
        'KuCoin Token',
        'Huobi Token',
        'GateToken',
        'OKB',
        'BNB',
        'Axie Infinity',
        'Decentraland',
        'The Sandbox',
        'Theta Network',
        'Enjin Coin',
        'Livepeer',
        'CEEK VR (Ticker missing)',
        'Illuvium',
        'Victoria VR',
        'Boson Protocol',
        'Yield Guild Games',
        'UFO Gaming',
        'BORA',
        'Reef',
        'Fantom',
        'AAVE',
        'Maker DAO',
        'Oasis Network',
        'Loopring',
        'yearn.finance',
        'Bancor',
        'Voyager Token',
        'Perpetual Protocol',
        '0x',
        'Synthetix',
        'Serum',
        'Request',
        'dydx',
        'Origin Protocol',
        'Terra',
        '1inch Network',
        'Curve DAO Finace',
        'Harmony',
        'EOS',
        'The Graph',
        'Secret',
        'Zilliqa',
        'Waves',
        'Decentralized Social',
        'Tether',
        'USD Coin',
        'Binance USD',
        'TerraUSD',
        'TrueUSD',
        'Pax Dollar',
        'Neutrino USD',
        'Fei USD',
        'Gemini Dollar',
        'Liquity USD',
        'HUSD',
        'Origin Dollar',
        'Filecoin',
        'IOTA',
        'Amp',
        'Chiliz',
        'Ankr',
        'Immutable X',
        'Audius',
        'Ocean Protocol',
        'Energy Web Token',
        'DigitalBits',
        'Constellation',
        'Centrifuge',
        'Dogecoin',
        'Shiba Inu',
        'Dogelon Mars',
        'Unibright',
        'GitCoin',
        'Balancer',
        'LiteCoin',
        'Raydium',
        'iExec RLC',
        'Kyber Network V2'
    ]

    return ticker_ls, name_ls

def main():

    ticker_ls, name_ls = get_coin_list()
    temp = ticker_ls
    
    final_string = str(temp[0])
    for i in range(1,len(temp)):
        final_string = final_string + ',' + str(temp[i])
    #final_string = 'BTC,ETH'
            
            
            
 
    api_key = "e3356473-3d97-4553-bfbd-0cd19f258718"
    cmc = CoinMarketCapAPI(api_key = api_key)
    data = cmc.cryptocurrency_quotes_latest(symbol = final_string)
    
    return data.data
    
    #return data.data
    #df = json.decoder(data.data, )
    df_final = pd.DataFrame(data.data)
    temp_df = pd.DataFrame(columns = df_final.columns)
    df_temp_temp = df_final.loc[df_final.index == 'quote']
    tmep_fff = pd.DataFrame(df_temp_temp.iat[0,0])
    
    for i in range(1,df_temp_temp.shape[1]):
        ttt_z = pd.DataFrame(df_temp_temp.iat[0,i])
        tmep_fff = pd.concat([tmep_fff, ttt_z], axis = 1)
    tmep_fff.columns = df_final.columns
    
    return tmep_fff
    return df_final

    temp2 = df_final.loc[df_final.index == 'quote']
#    temp3 = temp2.tolist()
#    temp4 = temp3[0]
    
    for i in range(0,temp2.shape[1]):
        data_temp = temp2[temp2.columns[i]].iat[0]
        data_temp = decoder(data_temp)
        return data_temp
    
    
    return df_final

