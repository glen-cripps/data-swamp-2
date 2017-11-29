import arrow
today_dt = arrow.now().format('YYYYMMDD')

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
df = sqlc.read.option("inferschema","true").option("header","true").csv("hdfs://localhost:54310/user/hduser/bitcoin." + today_dt + ".csv")
print(df.take(5))


col_list = df.columns

print(col_list)
col_list_replaced = [x.replace(' ', '_') for x in col_list]
col_list_replaced = [x.replace('(', '') for x in col_list_replaced]
col_list_replaced = [x.replace(')', '') for x in col_list_replaced]
col_list_replaced = [("r_" + x.lower()) for x in col_list_replaced]
print(col_list_replaced)

#df.withColumnRenamed(col_list,col_list_replaced)
df2 = df.toDF(*col_list_replaced)
df2.write.mode("overwrite").parquet("hdfs://localhost:54310/user/hduser/bitcoin." + today_dt + ".parquet")
# have some fun with columns containing funny characters tomorrow...
df2.show()
df2.printSchema()
#print(df.describe())
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

