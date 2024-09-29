---------------------------------------------------------->split and Substring in Pyspark<--------------------------------------

Split:-Split column into multiple dataframe columns.
pyspark.sql.functions provides a function split() to split dataframe string column into multiple columns, in this tutorial,you will learn how to split column into multiple columns using withColumn() and select() and also will explain how to use regular experssion(regex) on split function.

Following is the syntax of split() function. in order to use this first you need to import
pyspark.sql.functions.split

Syntax:

split(str,pattern,limit=-1)

Parameters:

str – a string expression to split
pattern – a string representing a regular expression.
limit –an integer that controls the number of times pattern is applied.
Note:  Spark 3.0 split() function takes an optional limit field. If not provided, the default limit value is -1.


Before we start with an example of PySpark split function, first let’s create a DataFrame and will use one of the column from this DataFrame to split into multiple colu
data = [('James','','Smith','1991-04-01'),
  ('Michael','Rose','','2000-05-19'),
  ('Robert','','Williams','1978-09-05'),
  ('Maria','Anne','Jones','1967-12-01'),
  ('Jen','Mary','Brown','1980-02-17')
]

columns=["firstname","middlename","lastname","dob"]
df=spark.createDataFrame(data,columns)

from pyspark.sql.functions import split
df=df.withColumn('year',split(df['dob'],'-').getItem(0))\
  .withColumn('month',split(df['dob'],'-').getItem(1))\
  .withColumn('day',split(df['dob'],'-').getItem(2))

df.show()
output:-
+---------+----------+--------+----------+----+-----+---+
|firstname|middlename|lastname|       dob|year|month|day|
+---------+----------+--------+----------+----+-----+---+
|    James|          |   Smith|1991-04-01|1991|   04| 01|
|  Michael|      Rose|        |2000-05-19|2000|   05| 19|
|   Robert|          |Williams|1978-09-05|1978|   09| 05|
|    Maria|      Anne|   Jones|1967-12-01|1967|   12| 01|
|      Jen|      Mary|   Brown|1980-02-17|1980|   02| 17|
+---------+----------+--------+----------+----+-----+---+
    
    
example:-2

data = [("John Doe", 25), ("Jane Smith", 45), ("Sam Brown", 22)]
columns = ["name", "age"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

df_split = df.withColumn("FirstName", split(df["name"], " ")[0]) \
             .withColumn("LastName", split(df["name"], " ")[1])
             
             

df_split.show()


output
+----------+---+---------+--------+
|      name|age|FirstName|LastName|
+----------+---+---------+--------+
|  John Doe| 25|     John|     Doe|
|Jane Smith| 45|     Jane|   Smith|
| Sam Brown| 22|      Sam|   Brown|
+----------+---+---------+--------+
