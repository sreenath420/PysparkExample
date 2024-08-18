------------------------------------------------>1.Piovt_Using<-----------------------------------------------------
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


---------------------------------------------------------------------------------2------------------------------------------------------------

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
------------------------------------3.different rows explode using----------------------------------------------------------
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
df_explode=df_c.withColumn('score',explode(col('score')))
----------------------------------------4.even numbers-------------------------------------------
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
StructField("id",IntegerType(),True)])
df=spark.createDataFrame(data=data,schema=schema)
df.show()

df1=df.filter(col("id")%2==0)
df1.show()
--------------------------..................5.word count------------------------------------------
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
   
----------------------------------------------------------------->6.Collect_list<-----------------------------------------------------
 Input     +----+-----+----+----------------------------
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


-----------------------------------------------------------------7.using union---------------------------------------
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


--------------------------------------------------------------->8. flattend nested json in pyspark <--------------------------------------------------------------------



df=spark.read.format("json").option("multiLine",True).\
  option("header",True).option("inferSchema",True).\
    load("dbfs:/FileStore/nested_json.json")
df.show(truncate=False)

what is option("multiLine",True) ?
To read JSON files that span multiple lines in PySpark, you can use the multiline option. 
This is useful for reading JSON files that are pretty-printed or contain nested structures.


+-------------------+---+----+--------------------------------------------+
|address            |id |name|phone_numbers                               |
+-------------------+---+----+--------------------------------------------+
|{New York, NY}     |1  |John|[{123-456-7890, home}, {987-654-3210, work}]|
|{San Francisco, CA}|2  |Jane|[{555-1234, mobile}, {777-4321, work}]      |
+-------------------+---+----+--------------------------------------------+




from pyspark.sql.functions import col,explode
df1=df.select(col('id'),col('name'),col('address.city').alias('city'),col('address.state').alias('state'),
          explode(col('phone_numbers')).alias('phone'))
df1.show()

+---+----+-------------+-----+--------------------+
| id|name|         city|state|               phone|
+---+----+-------------+-----+--------------------+
|  1|John|     New York|   NY|{123-456-7890, home}|
|  1|John|     New York|   NY|{987-654-3210, work}|
|  2|Jane|San Francisco|   CA|  {555-1234, mobile}|
|  2|Jane|San Francisco|   CA|    {777-4321, work}|

 
 df2=df1.select(col('id'),col('name'),col('city'),col('state'),col('phone.type').alias('phone_type'),col('phone.number').alias('phone_number'))
+---+----+-------------+-----+----------+------------+
| id|name|         city|state|phone_type|phone_number|
+---+----+-------------+-----+----------+------------+
|  1|John|     New York|   NY|      home|123-456-7890|
|  1|John|     New York|   NY|      work|987-654-3210|
|  2|Jane|San Francisco|   CA|    mobile|    555-1234|
|  2|Jane|San Francisco|   CA|      work|    777-4321|
+---+----+-------------+-----+----------+------------+



---------------------------------------------------------------> 9.explode function in json using pyspark <--------------------------------------------------------------------
Input-
+-------+---------------------+
|dept_id|e_id                 |
+-------+---------------------+
|101    |[10101, 10102, 10103]|
|102    |[10101, 10102]       |
+-------+---------------------+
Output-
+-------+------+
|dept_id|emp_id|
+-------+------+
|    101| 10101|
|    101| 10102|
|    101| 10103|
|    102| 10101|
|    102| 10102|
+-------+------+      
df=spark.read.json("dbfs:/FileStore/Practice/json_practice.json")
df.show(truncate=False)
from pyspark.sql.functions import * 
df1=df.select(col('dept_id'),explode(col("e_id")).alias("emp_id"))
df1.show()

--------------------------------------------------------------->10.read stream data<------------------------------------------------------

Convert the below line(consider this is your csv/txt file data) into a dataframe with 4 columns
input
emp1|20000|BA|BIGDATA|emp2|30000|BE|JAVA|emp3|40000|BE|TESTE

output

Name	Salary	Degree	Skill
emp1	20000	BA	BIGDATA
emp2	30000	BE	JAVA
emp3	40000	BE	TESTE

from pyspark.sql.functions import  *
from pyspark.sql.types import * 
data = "emp1|20000|BA|BIGDATA|emp2|30000|BE|JAVA|emp3|40000|BE|TESTE"
data_list=data.split('|')
data_reshaped=[data_list[i:i+4] for i in range(0,len(data_list),4)]

rdd = spark.sparkContext.parallelize(data_reshaped)
schema=StructType([StructField('employee',StringType(),True),
                   StructField('salary',StringType(),True),
                   StructField('cat',StringType(),True),
                   StructField('Tech',StringType(),True)])

df=spark.createDataFrame(rdd,schema)

df.show()

------------------------------------------------->11.max score of student in subject <--------------------------------------------------------
max marks of each student 
student table

student_name  maths  sciences   englies

a             90      85          60
b             56      89          67



  max_score
      90
      89


  from pyspark.sql.functions import greatest
data = [
    ("a", 90, 85, 60),
    ("b", 56, 89, 67)
]
columns=['studnet_name','maths','sciences','english']
df=spark.createDataFrame(data,schema=columns)


df_max=df.withColumn('max_score',greatest('maths','sciences','english'))
df_max.show()

--------------------------------->12.emp_intial_letter<--------------------------------------------

empid,   fname,  lname,  mgrid,  deptid,    salary
10,Kamal,        Kant,            20,     10,       10000
20,Rajesh,Das,,10,5000
30,Komal,kumar,20,20,3000
40,Ranjan,Desai,30,20,1000
50,Mohan,Kumar,20,20,6000
 output
initial count
KK       2
RD       2
MK       1
from pyspark.sql.functions import *
employee_data = [
    (10, 'Kamal', 'Kant', 20, 10, 10000),
    (20, 'Rajesh', 'Das', None, 10, 5000),
    (30, 'Komal', 'Kumar', 20, 20, 3000),
    (40, 'Ranjan', 'Desai', 30, 20, 1000),
    (50, 'Mohan', 'Kumar', 20, 20, 6000)
]

# Define schema for employees
employee_schema = ["empid", "fname", "lname", "mgrid", "deptid", "salary"]

# Create DataFrame
employee_df = spark.createDataFrame(employee_data, schema=employee_schema)
employee_df.show()
df=employee_df.withColumn('new_count',concat_ws("",substring(col('fname'),1,1),substring(col('lname'),1,1)))
df1=df.groupBy('new_count').count()
df1.show()


---------------------------->13.convert string values to numeric in pyspark<------------------------------------------
--->input<----
+------+
|salary|
+------+
|  1000|
|   ind|
|  2000|
|  4000|
|  5000|
|   usa|
+------+
--->output<--
   output|
+------+------+
|  1000|  1000|
|   ind|     0|
|  2000|  2000|
|  4000|  4000|
|  5000|  5000|
|   usa|     0|
+------+------+
creating a dataframe 
data=[(1000,),('ind',),(2000,),(4000,),(5000,),('usa',)]
schema=['salary']
df=spark.createDataFrame(data,schema=schema)
from pyspark.sql.functions import regexp_extract,col,when
df_numeric="^[0-9]+$"
#df1=df.withColumn('val',when(regexp_extract(col('salary'),df_numeric,0)=="",0)
df = df.withColumn("output", when(regexp_extract(col("salary"), df_numeric, 0) == "", 0).otherwise(col('salary').cast('integer')))




----------------------------->14.Employees Earning More than Managers<--------------------------------------------

  data = [
    (1, "Joe", 70000, 3),
    (2, "Henry", 80000, 4),
    (3, "Sam", 60000, None),
    (4, "Max", 90000, None)
]

# Define the schema
columns = ["Id", "Name", "Salary", "ManagerId"]
output:
+----+
|Name|
+----+
| Joe|
+----+

df=spark.createDataFrame(data,columns)
from pyspark.sql.functions import col
joindf=df.alias('employee').join(df.alias('manager'),col("employee.ManagerId")==col('manager.Id'),'inner')
df=joindf.filter(col("employee.Salary")>col("manager.Salary"))
df.select('employee.Name').show()

------------------------------------->15.Joining two dataframe<------------------------------------------

  ------------------------------------------>df1=FlightDetail<------------------------------ 
Origin	Destination		FlightID
O1				D1		1,2
O2				D2		3,4
O3				D3		5
 	 	 
--------------------------------------------------->df2=FlightMaster<----------------------------- 
FlightID	FlightName	 
1				F1	 
2				F2	 
3				F3	 
4				F4

output
+------+-----------+--------------------------------------+
|Origin|Destination|FlightName                            |
+------+-----------+--------------------------------------+
|    O1|         D1|                                 F1,F2|
|    O2|         D2|                                 F3,F4|
|    O3|         D3|                                      |
+------+-----------+--------------------------------------+


------------------------------------------------>16.pyspark_code<---------------------------

from pyspark.sql.functions import *
df1 = spark.createDataFrame([
    (("O1", "D1", "1,2")),
    (("O2", "D2", "3,4")),
    (("O3", "D3", "5"))
], ["Origin", "Destination", "FlightID"])

# df2 - FlightMaster
df2 = spark.createDataFrame([
    ((1, "F1")),
    ((2, "F2")),
    ((3, "F3")),
    ((4, "F4"))
], ["FlightID", "FlightName"])
df1=df1.withColumn('FlightID',explode(split(df1['FlightID'],',')))
df_joined=df1.join(df2,df1.FlightID==df2.FlightID,'left')
df_group=df_joined.groupBy('Origin','Destination').agg(concat_ws(',',collect_list('FlightName')))


Note:-
--------------------------------------------------->split<------------------------------------------------------
The split function in PySpark is used to split a string column in a DataFrame into multiple columns
syntax:
split(str, pattern, limit=-1)
example
df.withColumn('lastname_array', split(col('lastname'), ''))

--------------------------------------------------->explode<-----------------------------------------------------
The explode function in Apache Spark is used to convert an array or map column in a DataFrame into multiple rows, where each row represents an element 
in the array or a key-value pair in the map. Here are the key points and examples to help you understand how to use the explode function in PySpark.
Function: explode(array_or_map)
example
df_exploded = df.select(df["name"], explode(df["knownLanguages"]).alias("language"))

-------------------------------------------------->concat_ws<---------------------------------------------------------
The concat_ws function in PySpark is used to concatenate multiple columns with a user-specified separator.

Function: concat_ws(sep, *cols)
ex

df_concat = df.withColumn("fullname", concat_ws(" ", "firstname", "lastname"))

------------------------------------------------->collect_list<--------------------------------------------

The collect_list function in PySpark is used to collect all the values from a column into a single array. 
Function: collect_list(col)
example:-
df_with_list = df.groupBy("id").agg(collect_list("fruit").alias("fruits"))

--------------------------------------------->17.PyparkCode<------------------------------------------------------------
Input:-
   data = [
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5",
    "text1,text2|text3,text4|text5"
]
output:-
+-----+-----+-----+-----+-----+
|text1|text2|text3|text4|text5|
+-----+-----+-----+-----+-----+
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
|text1|text2|text3|text4|text5|
+-----+-----+-----+-----+-----+
# Create an RDD from the data
rdd = spark.sparkContext.parallelize(data)

# Split the data into columns
rdd_1=rdd.map(lambda x:x.replace('|',',').split(','))
columns=['text1','text2','text3','text4','text5']
rd=rdd_1.toDF(columns)
rd.show()

----------------------------------------------------------->Json_Extract<---------------------------------------------------
  Below json extract city and state 

from pyspark.sql.functions import *

data = [
    {
        "user": {
            "name": "Srikanth",
            "age": 30,
            "product": "Book 1",
            "address": {
                "city": "Hyd",
                "state": "TL"
            }
        }
    }
]

# Create DataFrame
df = spark.read.json(spark.sparkContext.parallelize(data))

df=df.select(col("user.address.city").alias('city'),col("user.address.state").alias('state'))


+----+-----+
|city|state|
+----+-----+
| Hyd|   TL|
+----+-----+

---------------------------------------------------->bonus less than 1000<-------------------------------------------------------------
 
name and bonus amount of each employee with a bonus less than 1000
in pyspark
  
  input: 
Employee table:
+-------+--------+------------+--------+
| empId | name   | supervisor | salary |
+-------+--------+------------+--------+
| 3     | Brad   | null       | 4000   |
| 1     | John   | 3          | 1000   |
| 2     | Dan    | 3          | 2000   |
| 4     | Thomas | 3          | 4000   |
+-------+--------+------------+--------+
Bonus table:
+-------+-------+
| empId | bonus |
+-------+-------+
| 2     | 500   |
| 4     | 2000  |
+-------+-------+
Output: 
+------+-------+
| name | bonus |
+------+-------+
| Brad | null  |
| John | null  |
| Dan  | 500   |
+------+-------+




employee_data = [(3, "Brad", None, 4000),
                 (1, "John", 3, 1000),
                 (2, "Dan", 3, 2000),
                 (4, "Thomas", 3, 4000)]

bonus_data = [(2, 500),
              (4, 2000)]

# Create DataFrames
employee_df = spark.createDataFrame(employee_data, ["empId", "name", "supervisor", "salary"])
bonus_df = spark.createDataFrame(bonus_data, ["empId", "bonus"])
employee_df = spark.createDataFrame(employee_data, ["empId", "name", "supervisor", "salary"])
bonus_df = spark.createDataFrame(bonus_data, ["empId", "bonus"])
df=employee_df.join(bonus_df,on='empId',how='left')
df_bonus=df.select('name','bonus').filter((col('bonus')<1000) | col('bonus').isNull())
df_bonus.show()



-------------------------------------------------------->cumlative_sum<---------------------------------------------------------------
data = [
    (3000, '22-may'),
    (5000, '23-may'),
    (8000, '25-may'),
    (10000, '22-june'),
    (12500, '3-july')
]
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("revenue", IntegerType(), True),
    StructField("date", StringType(), True)
])

output:-
|revenue|   date|month|  rev|
+-------+-------+-----+-----+
|  12500| 3-july| july|12500|
|  10000|22-june| june|10000|
|   3000| 22-may|  may| 3000|
|   5000| 23-may|  may| 8000|
|   8000| 25-may|  may|16000|

soluation:-
	from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Create DataFrame").getOrCreate()

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show the DataFrame
df.show()

from pyspark.sql.window import *
window_fun=Window.partitionBy('month').rowsBetween(Window.unboundedPreceding, Window.currentRow)
df=df.withColumn('rev',sum(col('revenue')).over(window_fun))
df.show()



----------------
