import arrow
today_dt = arrow.now().format('YYYYMMDD')

import sys
sys.path.append('/usr/local/spark/python')

from pyspark import SparkContext
from pyspark import HiveContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col

sc = SparkContext()
sqlc = HiveContext(sc)

f_full_path = "hdfs://localhost:54310" + "/bigdata/std/" + "all_stocks_hist." + today_dt + ".parquet"

df = sqlc.read.parquet(f_full_path)

df.show()

df.printSchema()

df.write.mode("overwrite").partitionBy("j_jobdate").saveAsTable("all_stocks_hist_" + today_dt )

pandazzz = df.toPandas()
#df.describe

df = sqlc.sql("show tables")
df.show()
