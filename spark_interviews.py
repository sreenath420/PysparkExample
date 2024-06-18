id name amout
1  A   30
1  B   80
1  A   50
2  B   30


id   A      B
1    80    80
2    0      30

how to write code by using dataframes


we can use here pivot
is a function that is used to transform data from long format to wide format. The pivot() function groups the data by a specific column and creates new columns based on the values in another column.

The syntax for using pivot() in PySpark is:

    pivot(column)
    
    

from pyspark.sql.functions import sum, col
data = [(1, 'A', 30),
        (1, 'B', 80),
        (1, 'A', 50),
        (2, 'B', 30)]

df = spark.createDataFrame(data, ["id", "name", "amount"])
df.printSchema()

pivot_df = df.groupBy("id").pivot("name").agg(sum("amount").alias("amount"))
pivot_df = pivot_df.fillna(0)
pivot_df.show()


---------------------------------------------------------------------------------------------------------------------------------------------

Input-

name    sub      mark
Siva   Math       80
siva   Eng        85
Anu    Math       85
Anu    Eng        80
Rahul  Math       90
Rahul  Eng        85

Output-S

Name Math Eng
Siva 80 85
Anu  85 80
Rahul 90 85

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data=[('siva','Math',80),
      ('siva','Eng',85),
      ('Anu','Math',85),
	  ('Anu','Eng',80),
	  ('Rahul','Math',90),
	  ('Rahul','Eng',85)]
	  
schema = StructType([
       StructField("name",StringType(),True), \
	   StructField("sub",StringType(),True), \
	   StructField("mark",IntegerType(),True)])	 

df = spark.createDataFrame(data=data,schema=schema)
df.show()

solution 1-----------------------
from pyspark.sql.functions import first, last,sum,max
df1=df.groupBy('name').pivot('sub').max('mark').alias('marks').orderBy('name').show()


df.createOrReplaceTempView('df_stud')

solution 2----------------------
df2=spark.sql("""select 
name,
sum(case when sub='Math' then mark end) as Math,
sum(case when sub='Eng' then mark end) as Eng
from df_stud
group by name """)
df2.show()
------------------------------------different rows----------------------------------------------------------
+----+------+-------------+
|  id|  name|        score|
+----+------+-------------+
|1111|sachin|[60, 120, 80]|
+----+------+-------------+


+----+------+---+
|  id|  name|col|
+----+------+---+
|1111|sachin| 60|
|1111|sachin|120|
|1111|sachin| 80|
+----+------+---+


from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
data = [(1111, 'sachin', [60, 120, 80])]
schema=StructType([
StructField("id",IntegerType(),True),\
StructField("name",StringType(),True),\
StructField("score", ArrayType(IntegerType()),True),\
])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

------------------------even numbers-------------------------------------------
input-
+---+
| id|
+---+
|  1|
|  2|
|  3|
|  4|
|  5|
|  6|
+---+
output-
+---+
| id|
+---+
|  2|
|  4|
|  6|
+---+

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
data = [(1,), (2,), (3,), (4,), (5,), (6,)]
schema=StructType([
StructField("id",IntegerType(),True),\

])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

df1=df.filter(col("id")%2==0)
df1.show()
--------------------------word count------------------------------------------
Input-
This is SreeNath
This is Ujjwala
Output-
(This,2)
(is,2)
(SreeNath,1)
(Ujjwala,1)
data = ["This is SreeNath", "This is Ujjwala"]
rdd =sc.parallelize(data)
counts=rdd.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)
##word_tuples_rdd.reduceByKey(lambda a, b: a + b)

counts.collect()
[('This', 2), ('SreeNath', 1), ('is', 2), ('Ujjwala', 1)]   
   
--------------------------------------------------------------------
 Input     +----+-----+----+
|item|sales|date|
+----+-----+----+
| 123|  100|1/31|
| 123|   50|1/29|
| 456|   50|1/31|
+----+-----+----+
output
+----+------------------+-------------------+
|item|collect_list(date)|collect_list(sales)|
+----+------------------+-------------------+
| 123|      [1/31, 1/29]|          [100, 50]|
| 456|            [1/31]|               [50]|
+----+------------------+-------------------+


from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
data = [(123,100,'1/31'),\
        (123,50,'1/29'),\
        (456,50,'1/31')]
schema=StructType([
StructField("item",IntegerType(),True),\
StructField("sales",IntegerType(),True),\
  StructField("date",StringType(),True),\
])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

from pyspark.sql.functions import *
df1 = df.groupBy("item").agg(collect_list("date"),collect_list("sales"))
df1.show()


--------------------------------------------------------------------------------------------------------
+----+-------+------+
|item| buyer1|buyer2|
+----+-------+------+
| 123|Richard|Satish|
+----+-------+------+

+----+-------+
|item| buyer1|
+----+-------+
| 123|Richard|
| 123| Satish|
+----+-------+

from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
data = [(123,'Richard','Satish')]
schema=StructType([
StructField("item",IntegerType(),True),\
StructField("buyer1",StringType(),True),\
  StructField("buyer2",StringType(),True)
])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

from pyspark.sql.functions import *
df1 = df.select('item','buyer1').union(df.select('item','buyer2'))
df1.show()
