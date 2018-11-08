import os

# 100 - get data from disparate sources, save local
os.system('python 100_all_stocks_pull.py')
os.system('python 100_google_news_pull.py')
os.system('python 100_apple_pull.py')
os.system('python 100_bitcoin_pull.py')
os.system('python 100_blackberry_pull.py')

# 200 - save local data to hdfs as a file
os.system('python 200_apple_hdfs.py')
os.system('python 200_bitcoin_hdfs.py')
os.system('python 200_blackberry_hdfs.py')

# 300 - load in parquet with columns, datatypes
os.system('python 300_apple_schema.py')
os.system('python 300_bitcoin_schema.py')
os.system('python 300_blackberry_schema.py')

# 400 - routines to save parquet files to hive

# 500 - routines to do ETL as needed for ML or BI

# 600 - machine learning model creation and scoring
os.system('python 500_google_news_stock_predict.py')

# 700 - machine learning prediction on live data

# 800 - scoring and results accounting

# 900 - use as needed for additional requirements...
