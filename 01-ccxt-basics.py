import ccxt
import datetime
import pandas as pd
import time

# Use and load coinbasepro markets
coinbase = ccxt.coinbasepro()
coinbase.load_markets()

def print_header():
    print()
    print('************************************************************************')
    print('*                  FETCHING NEW MARKET DATA: BTC/USDC                  *')
    print('*                      % s                      *' % datetime.datetime.now())
    print('************************************************************************')
    print()

while (True):
    print_header()
    # Fetch some candles
    ohlcv = coinbase.fetch_ohlcv('BTC/USDC', timeframe='5m', limit=10)
    # Define column headers
    columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    # Put candles into pandas dataframe
    df = pd.DataFrame(ohlcv, columns=columns)

    # convert unix epoch timestamp to datetime using apply()
    df['Timestamp'] = df['Timestamp'].apply(
        lambda ts: datetime.datetime.fromtimestamp(ts / 1000.0))

    # print dataframe
    print(df)
    time.sleep(10)

