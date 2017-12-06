data_dic = dict(name='blackberry',
                stock_ticker='bbry')

env_dic = dict(dumpster_path='/data/dumpster/',
               bigly_box='hdfs://localhost:54310',
               bigly_rdl="/bigdata/rdl",
               bigly_std="/bigdata/std")


import sys
import os
print __file__
print sys.argv[0]
print os.path.basename(__file__)
print os.path.basename(sys.argv[0])


# pip install pandas-datareader
import pandas as pd
#import pandas.io.data as web  # Package and modules for importing data; this code may change depending on pandas version
import pandas_datareader.data as web

#import pandas_datareader.data
#import pandas.web as web
import datetime

# We will look at stock prices over the past year, starting at January 1, 2016
start = datetime.datetime(2016, 1, 1)
end = datetime.date.today()

# Let's get Apple stock data; Apple's ticker symbol is AAPL
# First argument is the series we want, second is the source ("yahoo" for Yahoo! Finance), third is the start date, fourth is the end date
obj = web.DataReader(data_dic['stock_ticker'], "yahoo", start, end)
print(type(obj))
print(obj)

obj.to_csv(env_dic['dumpster_path'] + data_dic['name'] + ".csv")
