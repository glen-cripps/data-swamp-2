# So, I wanted to use spark objects to copy the raw file into hadoop
# But it turns out that spark really sucks at doing this thru its object library
# the easiest way is to just call hadoop command line style and do the file operations
# This script takes the daily drop into hadoop's raw layer
# In the raw layer, it's just files with date/timestamps, that's all we do here

# Arrow is our current favorite date bullshit package
import arrow
today_dt = arrow.now().format('YYYYMMDD')

local_path = "/data/daily_drop/bitcoin.csv"
hdfs_path = "/user/hduser/bitcoin." +  today_dt + ".csv"

import subprocess
subprocess.call(["hadoop", "fs", "-rm", "-f", hdfs_path])
subprocess.call(["hadoop", "fs", "-put", local_path, hdfs_path])


