from env_config import env_dic

data_dic = dict(name='blackberry',
                stock_ticker='bbry')


# Arrow is our current favorite date bullshit package
import arrow
today_dt = arrow.now().format('YYYYMMDD')

local_path = env_dic['local_path'] + "/" + data_dic['name'] +  ".csv"
hdfs_path = env_dic['server_rdl'] + "/" + data_dic['name'] + "." +  today_dt + ".csv"

import subprocess
subprocess.call(["/usr/local/hadoop/bin/hadoop", "fs", "-rm", "-f", hdfs_path])
subprocess.call(["/usr/local/hadoop/bin/hadoop", "fs", "-put", local_path, hdfs_path])


#get_blackberry
#save_blackberry
#schema_blackberry
