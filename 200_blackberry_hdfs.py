data_dic = dict(name='blackberry',
                stock_ticker='bbry')

env_dic = dict(dumpster_path='/data/dumpster/',
               bigly_box='hdfs://localhost:54310',
               bigly_rdl="/bigdata/rdl",
               bigly_std="/bigdata/std")
# Arrow is our current favorite date bullshit package
import arrow
today_dt = arrow.now().format('YYYYMMDD')

local_path = env_dic['dumpster_path'] + "/" + data_dic['name'] +  ".csv"
hdfs_path = env_dic['bigly_rdl'] + "/" + data_dic['name'] + "." +  today_dt + ".csv"

import subprocess
subprocess.call(["hadoop", "fs", "-rm", "-f", hdfs_path])
subprocess.call(["hadoop", "fs", "-put", local_path, hdfs_path])


