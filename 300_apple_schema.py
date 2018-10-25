import pandas as pd
desired_width = 320
pd.set_option('display.width', desired_width)

import arrow
today_dt = arrow.now().format('YYYYMMDD')

import sys
sys.path.append('/usr/local/spark/python')

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col

sc = SparkContext()
sqlc = SQLContext(sc)

# THIS is the RDD way... but once you read it you cant save it in parquet without parsing
# RDDS are prehistoric.  No concept of column names.  dont use the stupid things
#csv_rdd = sc.textFile("hdfs://localhost:54310/user/hduser/bitcoin." + today_dt + ".csv")
#print(csv_rdd.collect())
# Much easier to read the CSV with the header and then write to parquet using the DataFrame library..
f_directory = "hdfs://localhost:54310/bigdata/rdl/"
f_filename = "apple." + today_dt + ".csv"
f_filepath = f_directory + f_filename

# Much easier to read the CSV with the header and then write to parquet using the DataFrame library..
df = sqlc.read.option("inferschema","true").option("header","true").csv(f_filepath)
print(df.take(5))

# First rename all the columns to lowercase and remove spaces and special characters/
# raw data columns are prefixed with d_
col_list = df.columns
print(col_list)
col_list_replaced = [x.replace(' ', '_') for x in col_list]
col_list_replaced = [x.replace('(', '') for x in col_list_replaced]
col_list_replaced = [x.replace(')', '') for x in col_list_replaced]
col_list_replaced = [("r_" + x.lower()) for x in col_list_replaced]
# if you have duplicate column names as part of this step, you need to deal with that here
print(col_list_replaced)

df2 = df.toDF(*col_list_replaced)

## now you store all the attributes about the infile that you can collect with prefix f_
from pyspark.sql.functions import lit
df3 = df2.withColumn("f_filepath", lit(f_filepath)) # lit() function
df3 = df3.withColumn("f_directory", lit(f_directory)) # lit() function
df3 = df3.withColumn("f_filename", lit(f_filename))
df3 = df3.withColumn("f_unixuser", lit("hduser"))
df3 = df3.withColumn("f_unixgroup", lit("hdgroup"))
df3 = df3.withColumn("f_filebytes", lit(123.0)) # example to write a number field column
from datetime import datetime
df3 = df3.withColumn("f_filedate", lit(datetime.strptime(today_dt, '%Y%m%d'))) # example to write a date field
df3.show()
## now you store all the attributes about this process with prefix j

df4 = df3.withColumn("j_sysdate", lit(arrow.now().timestamp))

f_directory_out = "hdfs://localhost:54310/bigdata/std/"
f_filename_out = "apple." + today_dt + ".parquet"
f_filepath_out = f_directory_out + f_filename_out

df4.write.mode("overwrite").parquet(f_filepath_out)
# have some fun with columns containing funny characters tomorrow...
df4.show()
df4.printSchema()


df5 = df4.toPandas()


print(df.describe())
#df.show()
#from pyspark.sql import HiveContext
#hc = HiveContext(sc)
#df.write.format("orc").saveAsTable("bitcoin")
#df.write.format("csv").save("hdfs://localhost:54310/user/hduser/bitcoin_saved." + today_dt + ".csv")


#mapped_rdd.saveAsTextFile('hdfs://localhost:54310/user/hduser/bitcoin.20171118.txt')

#exprs = [col(column).alias(column.replace(' ', '_')) for column in df.columns]
#print(exprs)
#df2 = df.select(*exprs).collect
#df2.write.parquet("hdfs://localhost:54310/user/hduser/bitcoin." + today_dt + ".parquet")


#oldColumns = df.schema.names
#newColumns = exprs

#df2 = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), df)

#print(df2.columns)
#df2.write.parquet("hdfs://localhost:54310/user/hduser/bitcoin." + today_dt + ".parquet")

