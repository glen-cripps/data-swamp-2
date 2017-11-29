import arrow
today_dt = arrow.now().format('YYYYMMDD')

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import col



sc = SparkContext()

# Much easier to read the CSV with the header and then write to parquet using the DataFrame library..
f_full_path = "hdfs://localhost:54310/user/hduser/bitcoin." + today_dt + ".parquet"
f_filename = "bitcoin." + today_dt + ".parquet"

df = sqlc.read.parquet(f_full_path)

df.show()
df.printSchema()

from pyspark.sql.functions import lit
df2 = df.withColumn("f_full_path", lit(f_full_path)) # lit() function
df2 = df2.withColumn("f_filename", lit(f_filename))

df2.show()
df2.printSchema()
