-------------------------------------------------------------------------> Day1 <-----------------------------------------------------------------------------------

write a pyspark prgoram for a report that provides the pairs(actor_id,director_id) where the actor has cooperated with the direcor at least 3 times

input:-
+-------+----------+---------+
|ActorID|Directorid|timestamp|
+-------+----------+---------+
|      1|         1|        0|
|      1|         1|        1|
|      1|         1|        2|
|      1|         2|        3|
|      1|         2|        4|
|      2|         1|        5|
|      2|         1|        6|
+-------+----------+---------+

output:-


+-------+----------+
|ActorID|Directorid|
+-------+----------+
|      1|         1|
+-------+----------+


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType
schema=StructType([
  StructField("ActorID",IntegerType()),
  StructField("Directorid",IntegerType()),
  StructField("timestamp",IntegerType())
])

data=[
  (1,1,0),
  (1,1,1),
  (1,1,2),
  (1,2,3),
  (1,2,4),
  (2,1,5),
  (2,1,6)
]


spark=SparkSession.builder.getOrCreate()
df=spark.createDataFrame(data,schema)

df2_group=df.groupBy('ActorID','Directorid').count()
df2_group.show()

+-------+----------+-----+
|ActorID|Directorid|count|
+-------+----------+-----+
|      1|         1|    3|
|      1|         2|    2|
|      2|         1|    2|
+-------+----------+-----+


df_filter=df2_group.select("ActorID","Directorid").filter(df2_group['count']>2).show()

+-------+----------+
|ActorID|Directorid|
+-------+----------+
|      1|         1|
+-------+----------+

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
i/p-------------------------

+---------+---------+
|StudentID|ClassName|
+---------+---------+
|        A|     Math|
|        B|  English|
|        C|     Math|
|        D|  Biology|
|        E|     Math|
|        F| Computer|
|        G|     Math|
|        H|     Math|
|        I|     Math|
+---------+---------+

o/p-------------------------
+---------+-----+
|ClassName|count|
+---------+-----+
|     Math|    6|
+---------+-----+

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data = [
('A', 'Math'),
('B', 'English'),
('C', 'Math'),
('D', 'Biology'),
('E', 'Math'),
('F', 'Computer'),
('G', 'Math'),
('H', 'Math'),
('I', 'Math')]

schema = StructType([
StructField ("StudentID", StringType(), True),\
StructField ("ClassName", StringType(), True)
])

df = spark.createDataFrame(data=data,schema=schema)
df.show()

df.createOrReplaceTempView("df_view")
---------------------------------------------------------------spark sql-----------------------------------------------------------------
df=spark.sql("""select ClassName,count(StudentId) as count From df_view group by ClassName having count(StudentID)> 4""")
-----------------------------------------------------------------pyspark---------------------------------------------------------------
from pyspark.sql.functions import *
df3=df.groupBy("ClassName").count()
df3.filter("count > 4").show()
