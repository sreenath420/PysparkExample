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


-------------------------------------------------------------------------------------------------------------------------------------
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

-------------------------------------------------------removing spaces---------------------------------------------------------------------------
3.Regression Replace
input-
+----------------------+
|Full_name             |
+----------------------+
|Sreenath  Gooty       |
|Chandrakant  Narayan  |
|  Siva  Kumar Pillai  |
+----------------------+

+--------------------+

output-
+-------------------+
|    normalized_name|
+-------------------+
|           Sreenath|
|Chandrakant Narayan|
|  Siva Kumar Pillai|
+-------------------+

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import *
schema=StructType([
StructField("Full_name",StringType(),True)])

df=spark.createDataFrame(data=data1,schema=schema)
df_cleaned = df.withColumn("cleaned_name", regexp_replace(trim(df["Full_name"]), "\\s+", " "))
df_cleaned.show()

df1=spark.sql("""SELECT 
REGEXP_REPLACE(TRIM(Full_name), ' +', ' ') AS normalized_name
FROM df_view """)
df1.show()


-------------------------------------------------find the topper in each semester------------------------------------------------------------------------------
Input-
+---+----+--------+---------+-----+
| id|name|semester|  subject|marks|
+---+----+--------+---------+-----+
|  1|   A|       1|  PHYSICS|  100|
|  1|   A|       2|  PHYSICS|  150|
|  1|   A|       3|  PHYSICS|  200|
|  1|   A|       4|  PHYSICS|  250|
|  1|   A|       1|CHEMISTRY|   50|
|  1|   A|       2|CHEMISTRY|  250|
|  1|   A|       3|CHEMISTRY|  200|
|  1|   A|       4|CHEMISTRY|  350|
|  2|   B|       1|  PHYSICS|  150|
|  2|   B|       2|  PHYSICS|  250|
|  2|   B|       3|  PHYSICS|  100|
|  2|   B|       4|  PHYSICS|  200|
|  2|   B|       1|CHEMISTRY|  150|
|  2|   B|       2|CHEMISTRY|  150|
|  2|   B|       3|CHEMISTRY|  250|
|  2|   B|       4|CHEMISTRY|  300|
+---+----+--------+---------+-----+
ouput-
+----+--------+-----+------------+
|name|semester|marks|marks_window|
+----+--------+-----+------------+
|   B|       1|  300|           1|
|   A|       2|  400|           1|
|   B|       2|  400|           1|
|   A|       3|  400|           1|
|   A|       4|  600|           1|
+----+--------+-----+------------+

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data =[
(1,'A',1,'PHYSICS',100),
(1,'A',2,'PHYSICS',150),
(1,'A',3,'PHYSICS',200),
(1,'A',4,'PHYSICS',250),
(1,'A',1,'CHEMISTRY',50),
(1,'A',2,'CHEMISTRY',250),
(1,'A',3,'CHEMISTRY',200),
(1,'A',4,'CHEMISTRY',350),
(2,'B',1,'PHYSICS',150),
(2,'B',2,'PHYSICS',250),
(2,'B',3,'PHYSICS',100),
(2,'B',4,'PHYSICS',200),
(2,'B',1,'CHEMISTRY',150),
(2,'B',2,'CHEMISTRY',150),
(2,'B',3,'CHEMISTRY',250),
(2,'B',4,'CHEMISTRY',300)]
schema=StructType([
StructField("id",IntegerType(),True),\
StructField("name",StringType(),True),\
StructField("semester",IntegerType(),True),\
StructField("subject",StringType(),True),\
StructField("marks",IntegerType(),True)
])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank
sem_marks=df.groupBy(df.name,df.semester)\
  .agg(sum(df.marks).alias('marks'))\
   .orderBy(df.semester) 
windowSpec = Window.partitionBy(sem_marks.semester).orderBy(sem_marks.marks.desc())
window_df=sem_marks.withColumn('marks_window',dense_rank().over(windowSpec))
result=window_df.filter(window_df.marks_window == 1)
result2 = result.select(result.name,result.semester,result.marks).show()
result2.show()
