#100-mydataname.py
# 100 series programs get data from external sources
# They only need to read whatever data they can get and save local into the daily drop folder
# dates and etc is not to managed here, just get as much current data as is manageable and save it to a file
# cleanup processes will ensure the disk is managed provided we stick to our naming conventions

import pickle
import quandl
local_file = '/data/daily_drop/bitcoin.pickle'
# function from internet to read data from quandl and return a pickle dataset
# yes i dont know wtf a pickle file is
#test
def get_quandl_data(quandl_id):
    '''Download and cache Quandl dataseries'''
    cache_path = '{}.pkl'.format(quandl_id).replace('/','-')
    try:
        f = open(cache_path, 'rb')
        df = pickle.load(f)
        print('Loaded {} from cache'.format(quandl_id))
    except (OSError, IOError) as e:
        print('Downloading {} from Quandl'.format(quandl_id))
        df = quandl.get(quandl_id, returns="pandas")
        df.to_pickle(cache_path)
        print('Cached {} at {}'.format(quandl_id, cache_path))
    return df

# Pull Kraken BTC price exchange data
btc_usd_price_kraken = get_quandl_data('BCHARTS/KRAKENUSD')

# show some data
print(btc_usd_price_kraken)

# write the raw pickle file locally
with open(local_file, 'wb') as f:
    pickle.dump(btc_usd_price_kraken, f)


btc_usd_price_kraken.to_csv("/data/daily_drop/bitcoin.csv")
btc_usd_price_kraken.to_json("/data/daily_drop/bitcoin.json")
#'btc_usd_price_kraken.to_parquet("/hadooper/daily_dump/bitcoin.parquet","fastparquet")



    #local_file = '/hadooper/daily_drop/bitcoin.parquet'
#btc_usd_price_kraken.write.parquet(local_file)
