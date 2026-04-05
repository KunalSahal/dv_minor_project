import asyncio
import yfinance as yf
from db_connect import stocks_collection
def get_finance_data(ticker):
    data=[]
    
    '''
    Data Extraction
    '''
    df=yf.Ticker(ticker).history(period='1d')
    
    '''
    Data Tranformation
    '''
    df=df.reset_index()
    list_df=df.values.tolist()
    
    for val in list_df:
        pub_date=str(val[0]).split(' ')
        pub_date=pub_date[0]
        low=round(val[3], 2)
        high=round(val[2], 2)
        open=round(val[1], 2)
        close=round(val[4], 2)
        volume=val[5]
        temp=pub_date.split('-')
        data.append({'_id': temp[2]+temp[1]+temp[0]+ticker,'ticker':ticker, 'pub_date':pub_date, 'low':low, 'high':high, 'open':open, 'close':close, 'volume':volume})
        
    return data

# Conflict Catalysts (Directly Affected)
crude_oil_ticker='USO'
lockheed_martin_ticker='LMT'
raytheon_ticker='RTX'
general_dynamics_ticker='GD'
natural_gas_ticker='UNG'

crude_oil_data=get_finance_data(crude_oil_ticker)
lockheed_martin_data=get_finance_data(lockheed_martin_ticker)
raytheon_data=get_finance_data(raytheon_ticker)
general_dynamics_data=get_finance_data(general_dynamics_ticker)
natural_gas_data=get_finance_data(natural_gas_ticker)

# Safe Havens (Risk Aversion)
gold_ticker='GLD'
yen_ticker='JPY=X'
'''CBOE Interest Rate 10 Year T No'''
CBOE_ticker='^TNX'

gold_data=get_finance_data(gold_ticker)
yen_data=get_finance_data(yen_ticker)
CBOE_data=get_finance_data(CBOE_ticker)

# Market Baselines (Economic Sentiment)
'''S&P 500: benchmark for the U.S. and global equity markets.'''
SP_ticker='^GSPC'
'''Nikkei 225: Asian trade hubs'''
Nikkei_ticker='^N225'
'''"Fear Gauge," it measures market volatility and investor uncertainty.'''
VIX_ticker='^VIX'

SP_data=get_finance_data(SP_ticker)
Nikkei_data=get_finance_data(Nikkei_ticker)
VIX_data=get_finance_data(VIX_ticker)

final_data=[crude_oil_data, lockheed_martin_data, raytheon_data, general_dynamics_data, natural_gas_data, gold_data, yen_data, CBOE_data, SP_data, Nikkei_data, VIX_data]

'''
Data Loading
'''
async def insert_data(filtered_data):
    for data in filtered_data:
        await stocks_collection.update_one( 
            {
                '_id': data[0]['_id']
            },
            {
                '$set': data[0]
            }, 
            upsert=True
        )
    
async def main():
    await insert_data(final_data)

asyncio.run(main())
