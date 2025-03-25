import yfinance as yf
import pandas as pd
import numpy as np
import datetime
import os

def get_yf_tickers(tickers, verbose=True, intraday=False, interval='5m'):
    """
        Pull tickers using the yfinance API. Pulls daily corrected closing values by default, but intraday values with
        specified interval can be pulled with intraday=True.
    """
    t_f = str(datetime.date.today())
    t_i = str(datetime.date.today()-datetime.timedelta(days=59))
    if verbose:
        print(f'intraday start: t_i = {t_i} \nintraday stop: t_f = {t_f} (today)')

    df = pd.DataFrame(index=None)

    if intraday:
        for ticker in tickers:
            if ticker in list(df.columns):
                continue
            data = yf.download(ticker, start=t_i, end=t_f, interval=interval)
            df[ticker] = data['Close']

        df = df.dropna(axis=1)

        return df

    else:
        for ticker in tickers:
            if ticker in list(df.columns):
                continue
            data = yf.download(ticker, start='2023-01-01', end=t_f)
            df[ticker] = data['Close']

        df = df.dropna(axis=1)
        
        return df

if __name__ == '__main__':

    tickers = ['PSI','NVDA','AVGO','LRCX','ADI','QCOM','TXN','AMAT','MU','ACMR','NVMI','QQQ', \
           'SPXT','BRK-B','VDE','VNQ','VHT','VTI','VXUS','XLE','XLI','XLF','XLK','XLY',   \
           'XLP','XLV', 'XLB', 'GDX', 'XOP', 'IYR', 'XHB', \
           'AAPL', 'MSFT', 'GOOG', 'META', 'TSLA']

    df = get_yf_tickers(tickers)
    out_file = 'data/'+'tickers__' +str(datetime.date.today()).replace('-','_') + '__.pickle'
    df.to_pickle(out_file)
