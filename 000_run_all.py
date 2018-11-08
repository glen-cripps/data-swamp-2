import os

os.system('python 100_all_stocks_pull.py')
os.system('python 100_google_news_pull.py')
os.system('python 100_apple_pull.py')
os.system('python 100_bitcoin_pull.py')
os.system('python 100_blackberry_pull.py')

os.system('python 200_apple_hdfs.py')
os.system('python 200_bitcoin_hdfs.py')
os.system('python 200_blackberry_hdfs.py')

os.system('python 300_apple_schema.py')
os.system('python 300_bitcoin_schema.py')
os.system('python 300_blackberry_schema.py')


os.system('python 500_google_news_stock_predict.py')

