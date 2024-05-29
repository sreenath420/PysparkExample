Input-
+--------+
|Category|
+--------+
|       A|
|       B|
|       C|
|       A|
|       B|
+--------+
Output-
+----------------+
|DistinctCategory|
+----------------+
|               3|
+----------------+
1.Spark sql

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data=[
('A',),
('B',),
('C',),
('A',),
('B',)]

schema = StructType([
StructField("Category", StringType(), True)])

df=spark.createDataFrame(data=data,schema=schema)
df.show()

2.pyspark

from pyspark.sql.functions import *
df2=df.select(countDistinct("category").alias("DistinctCategory"))
df2.show()

