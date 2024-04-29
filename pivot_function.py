question 1----
id name amout
1  A   30
1  B   80
1  A   50
2  B   30

output---
id   A      B
1    80    80
2    0      30


from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data=[(1,"A",30),
      (1,"B",80),
	  (1,"A",50),
	  (2,"B",30)]
	  
schema = StructType([
       StructField("id",IntegerType(),True), \
	   StructField("name",StringType(),True), \
	   StructField("amout",IntegerType(),True)])

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()

id name amout
1  A   30
1  B   80
1  A   50
2  B   30

df1=df.groupBy('id').sum('amout').show()	

df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()

df1=df.groupBy('id').sum('amout').show()

+---+----------+
| id|sum(amout)|
+---+----------+
|  1|       160|
|  2|        30|
+---+----------+


df.groupBy('id').pivot('name').sum('amout').show()

(7) Spark Jobs
+---+----+---+
| id|   A|  B|
+---+----+---+
|  1|  80| 80|
|  2|null| 30|
+---+----+---+

df.createOrReplaceTempView("fee")

df1=spark.sql("""select  id,sum(case when name='A' then amout end) as A,
              sum(case when name='B' then amout end) as B 
              from fee group by id""")
df1.show()

+---+----+---+
| id|   A|  B|
+---+----+---+
|  1|  80| 80|
|  2|null| 30|
+---+----+---+

question 2-----------------------------------------------------------------------------------------------------------------
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
