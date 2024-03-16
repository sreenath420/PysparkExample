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
