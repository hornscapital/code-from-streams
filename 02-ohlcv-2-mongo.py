import ccxt
import datetime
import pandas as pd
from pymongo import MongoClient  # create MongoClient
import time

# GET BTC/USDC CANDLE DATA
# fetch single record for now, disregard overlapping timestamps
# refer to 01-ccxt-basics.py for commented version of below code
coinbase = ccxt.coinbasepro()
coinbase.load_markets()

while(True):
    coinbase_ohlcv = coinbase.fetch_ohlcv('BTC/USDC', timeframe='5m', limit=1)
    columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(coinbase_ohlcv, columns=columns)
    df['Timestamp'] = df['Timestamp'].apply(
        lambda ts: str(datetime.datetime.fromtimestamp(ts / 1000.0)))
    # Set Timestamp as index
    df.index = df['Timestamp']
    print(df)

    # INSERT BTC/USDC CANDLE DATA INTO MONGO

    # automatically connect to default host and port
    ##client = MongoClient()
    # connect to specified host
    ##client = MongoClient('localhost', 27017)
    # connect to specified host with MongoDB URI format
    client = MongoClient('mongodb://localhost:27017/')

    # get database
    db = client.btcusdc
    # get collection
    ohlcv_collection = db.ohlcv

    # infinite loop, will add new document every 10 seconds
    # insert DataFrame as single document into btcusdc.ohlcv
    print('calling insert_one()')
    ohlcv_id = ohlcv_collection.insert_one(df.to_dict()).inserted_id
    print('ohlcv_id: ' + str(ohlcv_id))
    time.sleep(10)


# Normally would deleted commented out code
# but leaving the following for educational purposes

# iterate over DataFrame and insert each row into btcusdc.ohlcv
# for column_name, row in df.iteritems():
#     print('column_name: %s:' % column_name)
#     print('row: %s' % row)

# for row in df.iterrows():
#     print(type(row))
#     print(row)
#     print(dict(row))
#     ohlcv_id = ohlcv_collection.insert_one(dict(row)).inserted_id
#     print('inserted ohlcv_id: %s' % ohlcv_id)

# # fetch first document from ohlcv_collection
# print()
# print('calling find_one()')
# print(ohlcv_collection.find_one())

# # fetch document by query
# print()
# print('calling find_one({"volume": 537.23})')
# print(ohlcv_collection.find_one({"volume": 537.23}))

# # delete document by ohlcv_id
# print()
# print("calling delete_one({'_id': %s })" % str(ohlcv_id))
# print(ohlcv_collection.delete_one({'_id': ohlcv_id}))

# # try to access document to ensure it's really gone
# print()
# print('attempting to fetch deleted object ohlcv_id: %s' % str(ohlcv_id))
# print(ohlcv_collection.find_one())
