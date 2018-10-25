import sys
import os

print __file__
print sys.argv[0]
print os.path.basename(__file__)
print os.path.basename(sys.argv[0])

import pandas_datareader.data as web
import datetime
import pandas as pd

# for now let's just get NASDAQ working
exchanges = {
#    "nyse":"http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nyse&render=download",
#    "amex":"http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=amex&render=download",
    "nasdaq":"http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=nasdaq&render=download"
}

iter = 1
for exchange_key in exchanges:
    exchange_stocks = pd.DataFrame.from_csv(exchanges[exchange_key])
    exchange_stocks.sort_index(inplace=True)
    exchange_stocks["Exchange"] = exchange_key
    exchange_stocks["StockTicker"] = exchange_stocks.index
#    exchange_stocklist.append(exchange_stocks)
    if iter == 1:
        all_exchange_stocks = exchange_stocks.copy()
    else:
        all_exchange_stocks = all_exchange_stocks.append(exchange_stocks,ignore_index=True)
    iter = iter+1


#stocks = df.index.tolist()
print("brealk")
import arrow

#
# # We will look at stock prices over the past year, starting at January 1, 2016
start = datetime.datetime(2018, 9, 1)
end = datetime.date.today()
today_dt = arrow.now().format('YYYYMMDD')

#all_exchange_stocks = all_exchange_stocks.head(25)
iter = 1
for index, row in all_exchange_stocks.iterrows():
    if iter == 1:
        all_stocks_header = all_exchange_stocks[all_exchange_stocks.index==0]
        stock_hist = web.DataReader(row.StockTicker, "yahoo", start, end)
        stock_hist["Exchange"] = row.Exchange
        stock_hist["StockTicker"] = row.StockTicker
        all_stocks_hist = stock_hist.copy()
    else:
        try:
            print("processing " + row.Exchange + "-" + row.StockTicker)
            stock_hist = web.DataReader(row.StockTicker, "yahoo", start, end)
            all_stocks_header = all_stocks_header.append(row, ignore_index=True)
#            print(stock_hist.head())
            stock_hist["Exchange"] = row.Exchange
            stock_hist["StockTicker"] = row.StockTicker
            all_stocks_hist = all_stocks_hist.append(stock_hist,ignore_index=True)
        except:
            print("wtf unable to download " + row.Exchange + "-" + row.StockTicker)
            pass
    iter = iter + 1
    #    print(stockdata.head())


all_stocks_header.to_parquet('/data/dumpster/all_stocks_header.' + today_dt + '.parquet',compression='gzip')
all_stocks_hist.to_parquet('/data/dumpster/all_stocks_hist.' + today_dt + '.parquet',compression='gzip')

print(all_stocks_header.head())
print(all_stocks_hist.head())
print("End!")


#
#
# print(type(apple))
# print(apple)
#

#exchange_stocks.sort_values(by=StockTicker)
