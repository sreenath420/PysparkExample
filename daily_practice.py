1.distinctCount

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


--------------------------------------------------------------------------------------------------------------------------------------------------

Question: Given a DataFrame with columns FirstName and LastName, create a new column FullName by concatenating FirstName and LastName with a space
in between. Handle null values by excluding them from the concatenation.
Input :

[FirstName|LastName|
Aman| null|
null| Sharma |
Gagan| Singh |
+-
Output:
+---------+--------+-----------+
|firstname|lastname|   fullname|
+---------+--------+-----------+
|     Aman|    null|       Aman|
|     null|  sharma|     sharma|
|    Gagan|   singh|Gagan singh|
+---------+--------+-----------+
+



from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *
data=[
('Aman','null'),\
('null','sharma'),\
('Gagan','singh')
]

schema=StructType([
StructField("firstname",StringType(),True),\
StructField("lastname",StringType(),True)])
df=spark.createDataFrame(data=data,schema=schema)

df2=df.withColumn("firstname",when(col("firstname")=="null",None).otherwise(col("firstname")))\
.withColumn("lastname",when(col("lastname")=="null",None).otherwise(col("lastname")))

df3=df2.withColumn("fullname",concat_ws(' ',col('firstname'),col('lastname')))
df3.show()
