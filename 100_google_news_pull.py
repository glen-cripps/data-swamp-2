import sys
import os

print sys.argv[0]
print os.path.basename(sys.argv[0])

import pandas_datareader.data as web
import datetime
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup
import requests
import time
from random import randint
def scrape_news_summaries(s):
    time.sleep(randint(0, 2))  # relax and don't let google be angry
    print("http://www.google.com/search?q="+s+"&tbm=nws")
    r = requests.get("http://www.google.com/search?q="+s+"&tbm=nws&tbs=qdr:d")
    print(r.status_code)  # Print the status code
    content = r.text
    news_summaries = []
    soup = BeautifulSoup(content, "html.parser")
    st_divs = soup.findAll("div", {"class": "st"})
    for st_div in st_divs:
        news_summaries.append(st_div.text)
    print(news_summaries)
    return news_summaries

#l = scrape_news_summaries("NASDAQ:AAON")


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

    if iter == 1:
        all_exchange_stocks = exchange_stocks.copy()
    else:
        all_exchange_stocks = all_exchange_stocks.append(exchange_stocks,ignore_index=True)

    iter = iter+1

import arrow
#
# # We will look at stock prices over the past year, starting at January 1, 2016
start = datetime.date.today() - datetime.timedelta(days=5)
end = datetime.date.today()

today_dt = arrow.now().format('YYYYMMDD')

iter = 1
for index, row in all_exchange_stocks.iterrows():
    if iter == 1:
        all_stocks_header = all_exchange_stocks[all_exchange_stocks.index==0]
        stock_hist = web.DataReader(row.StockTicker, "yahoo", start, end)
        stock_hist.reset_index(inplace=True)
        stock_hist["Exchange"] = row.Exchange
        stock_hist["StockTicker"] = row.StockTicker
        all_stocks_hist = stock_hist.copy()
        news_all = pd.DataFrame(columns = ["ticker","fetch_dt","list_of_news"])
    else:
        l = scrape_news_summaries(row.Exchange + ":" + row.StockTicker)
        if len(l) >= 1:
            news = pd.DataFrame(columns = ["ticker","fetch_dt","list_of_news"])
            news['list_of_news']=l
            news['fetch_dt']=today_dt
            news['ticker']=row.StockTicker
            news_all = news_all.append(news, ignore_index=True)
        else:
            news = pd.DataFrame(columns = ["ticker","fetch_dt","list_of_news"])
            news = news.append(pd.Series([np.nan]), ignore_index = True)
            news['fetch_dt']=today_dt
            news['ticker']=row.StockTicker
            news_all = news_all.append(news, ignore_index=True)
        try:
            print("processing " + row.Exchange + "-" + row.StockTicker)
            stock_hist = web.DataReader(row.StockTicker, "yahoo", start, end)
    #        stock_hist['list_of_news'] = l
            stock_hist.reset_index(inplace=True)
            all_stocks_header = all_stocks_header.append(row, ignore_index=True)
            print(stock_hist.head())
            stock_hist["Exchange"] = row.Exchange
            stock_hist["StockTicker"] = row.StockTicker
            all_stocks_hist = all_stocks_hist.append(stock_hist,ignore_index=True)
        except:
            print("wtf unable to download " + row.Exchange + "-" + row.StockTicker)
            pass
    iter = iter + 1
#    if iter >= 20:
#        break
    #    print(stockdata.head())
news_all.drop(columns=0, axis=1, inplace=True)

news_all.to_parquet('/data/dumpster/all_stocks_news.' + today_dt + '.parquet',compression='gzip')

print(all_stocks_header.head())
print(all_stocks_hist.head())
print("End!")

