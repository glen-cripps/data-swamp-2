!#/bin/bash

/usr/local/hadoop/sbin/start-all.sh
/usr/local/spark/sbin/start-all.sh

cd /home/hduser/PycharmProjects/data-swamp/
/usr/bin/python2.7 100_all_stocks_pull.py
/usr/bin/python2.7 100_google_news_pull.py
/usr/bin/python2.7 100_apple_pull.py
/usr/bin/python2.7 100_bitcoin_pull.py
/usr/bin/python2.7 200_apple_hdfs.py
/usr/bin/python2.7 200_bitcoin_hdfs.py
/usr/bin/python2.7 300_apple_schema.py
/usr/bin/python2.7 300_bitcoin_schema.py
/usr/bin/python2.7 400_bitcoin_hive.py
/usr/bin/python2.7 500_google_news_stock_predict.py

