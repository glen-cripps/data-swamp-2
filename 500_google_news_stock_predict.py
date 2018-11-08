#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 31 11:49:29 2018

@author: hduser
"""
import pandas_datareader.data as web
import datetime
import pandas as pd
import numpy as np
import re
import nltk
from nltk.corpus import stopwords
import scipy.sparse
from sklearn.feature_extraction.text import CountVectorizer

today_dt        = datetime.date.today() - datetime.timedelta(days=1)
yday_dt    = today_dt - datetime.timedelta(days=1)
yyday_dt    = today_dt  - datetime.timedelta(days=2)
week_ago_dt     = today_dt  - datetime.timedelta(days=7)
month_ago_dt    = today_dt  - datetime.timedelta(days=30)

# Assumption is that I'm running this in the morning.

# So - using yesterday's closing stock price gain with yydays news (2 days ago) to train
# then using yesterday's news to predice tomorrow's gainers
stock_prices_header = pd.read_parquet('/data/dumpster/all_stocks_header.' + today_dt.strftime('%Y%m%d')  + '.parquet')
stock_prices_hist = pd.read_parquet('/data/dumpster/all_stocks_hist.' + today_dt.strftime('%Y%m%d')  + '.parquet')
stock_today_news = pd.read_parquet('/data/dumpster/all_stocks_news.' + today_dt.strftime('%Y%m%d')  + '.parquet')
stock_yday_news = pd.read_parquet('/data/dumpster/all_stocks_news.' + yday_dt.strftime('%Y%m%d')  + '.parquet')

# sort this bs later
stock_action_today = stock_prices_hist.query('Date=="' + today_dt.strftime('%Y%m%d') + '"')
stock_action_today['gain_pct'] = stock_action_today['Close']/stock_action_today['Open']
stock_action_today['class_gain'] = np.where(stock_action_today['gain_pct']>=1.10, 1 , 0)



# K - so - it's important to do the textual cleanup on the complete dataset of text
# ie 2 days of text right now.  Why?   Because you need the full vocabulary in the
# bag of words.  Not sure exactly if I really need it but I'mma go with it

stock_news = stock_today_news.append( stock_yday_news)

stock_news['utf'] = stock_news['list_of_news'].str.encode('utf-8')
stock_news['str'] = stock_news['utf'].astype(str) 
stock_news['chars'] = stock_news['str'].map(lambda x: re.sub(r'\W+', ' ', x))
stock_news['lower'] = stock_news['chars'].str.lower()
stock_news['word_list'] = stock_news['lower'].str.split()

stock_news_by_day = stock_news.groupby(['ticker','fetch_dt'])['word_list'].apply(list).reset_index()

def join_list(l):
    result = sum(l, [])
    return result

stock_news_by_day['word_unlist'] = stock_news_by_day['word_list'].apply(join_list)

my_dict = {}

for list1 in stock_news_by_day['word_list']:
    for list2 in list1:    
        for word in list2:
            my_dict[word] = my_dict.get(word, 0) + 1

nltk.download('stopwords')

stopwords = set(stopwords.words('english'))

clean_dict = my_dict
for k in my_dict.keys(): # iterate over word_list
  if k in stopwords or k.isdigit()==True or k[:1].isdigit()==True: 
    clean_dict.pop(k)

stock_news_by_day['word_list_no_stopwords'] = stock_news_by_day['word_unlist'].apply(lambda x: [item for item in x if item not in stopwords])
stock_news_by_day['word_list_no_sw_no_numbers'] = stock_news_by_day['word_list_no_stopwords'].apply(lambda x: [item for item in x if (item.isdigit()==False or item[:1].isdigit()==False)])
stock_news_by_day['not_list_no_stopwords'] = stock_news_by_day['word_list_no_sw_no_numbers'].apply(lambda x: ' '.join(x))

#8. Now, use the scikit-learn function CountVectorizer to create bag-of-words features.
vectorizer=CountVectorizer()
vectorizer.fit(clean_dict)
bag_of_words = vectorizer.transform(stock_news_by_day['not_list_no_stopwords'])

stock_news_by_day['bag_of_words'] = list(vectorizer.transform(stock_news_by_day['not_list_no_stopwords']).todense())
text_table =pd.DataFrame(vectorizer.transform(stock_news_by_day['not_list_no_stopwords']).toarray(),columns=vectorizer.get_feature_names())

stock_left = stock_news_by_day[['fetch_dt','ticker']]
stock_left.rename(columns = {'ticker':'StockTicker_l'}, inplace=True)

stock_right = text_table
stock_news = pd.concat([stock_left, stock_right], axis=1)

stock_news_yday = stock_news.query('fetch_dt=="' + yday_dt.strftime('%Y%m%d') + '"')
stock_news_today = stock_news.query('fetch_dt=="' + today_dt.strftime('%Y%m%d') + '"')


stock_today = stock_action_today[['StockTicker','class_gain']]
stock_yday = stock_news_yday

stock_train = stock_today.merge(right = stock_yday, right_on='StockTicker_l', left_on = 'StockTicker')
stock_train.drop("StockTicker_l", inplace=True, axis=1)

from sklearn.naive_bayes import GaussianNB
from sklearn.naive_bayes import MultinomialNB

algorithm_a=GaussianNB()

stock_train_X = stock_train.drop('StockTicker', axis=1)
stock_train_X = stock_train_X.drop('fetch_dt', axis=1)

stock_test_X = stock_news_today.drop('StockTicker_l', axis=1)
stock_test_X = stock_test_X.drop('fetch_dt', axis=1)

algorithm_a.fit(stock_train_X.drop('class_gain',axis=1) , stock_train['class_gain'] )

#testing your results
stock_news_today['aa_predict'] = algorithm_a.predict(stock_test_X)
lookyme = stock_news_today.query('aa_predict==1')



stock_pickem = 'FLWS'

pickem_price_hist = web.DataReader(stock_pickem, "yahoo", month_ago_dt, today_dt)
pickem_action = stock_prices_hist.query('StockTicker=="' + stock_pickem + '"')
pickem_header = stock_prices_header.query('StockTicker=="' + stock_pickem + '"')

pickem_news = stock_yday_news.query('ticker=="' + stock_pickem + '"')
