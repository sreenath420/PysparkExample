1./*You are given a dataframe with columns id, email, sent_date. The email id is a frequently updated column in this dataframe. Your task is to find out latest email of the students based on the most recent update_date */


input
+---+-----+-----------------+-------------------+
| id| name|            email|        update_date
+---+-----+-----------------+-------------------+
|  1| john| john@example.com|2023-09-01 10:30:00|
|  2| jane| jane@example.com|2023-09-02 15:45:00|
|  1| john| john@example.org|2023-09-03 15:45:00|
|  2| jane| jane@example.org|2023-09-04 15:45:00|
|  3|  bob|  bob@example.com|2023-09-05 15:45:00|
|  4|alice|alice@example.org|2023-09-06 10:30:00|
|  5|  eve|    eve@gmail.com|2023-09-07 15:45:00|
|  4|alice|  alice@gmail.com|2023-09-08 15:45:00|
|  5|  eve|  eve@example.com|2023-09-09 15:45:00|
+---+-----+-----------------+-------------------+
output
+---+-----+----------------+-------------------+
| id| name|           email|        update_date|
+---+-----+----------------+-------------------+
|  1| john|john@example.org|2023-09-03 15:45:00|
|  2| jane|jane@example.org|2023-09-04 15:45:00|
|  3|  bob| bob@example.com|2023-09-05 15:45:00|
|  4|alice| alice@gmail.com|2023-09-08 15:45:00|
|  5|  eve| eve@example.com|2023-09-09 15:45:00|
+---+-----+----------------+-------------------+


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window

# Initialize SparkSession
spark = SparkSession.builder \
 .appName("CreateDataFrameFromSQLTable") \
 .getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
 StructField("id", IntegerType(), nullable=False),
 StructField("name", StringType(), nullable=False),
 StructField("email", StringType(), nullable=False),
 StructField("update_date", StringType(), nullable=False)
])

# Create a list of tuples containing the data
data = [
 (1, 'john', 'john@example.com', '2023-09-01 10:30:00'),
 (2, 'jane', 'jane@example.com', '2023-09-02 15:45:00'),
 (1, 'john', 'john@example.org', '2023-09-03 15:45:00'),
 (2, 'jane', 'jane@example.org', '2023-09-04 15:45:00'),
 (3, 'bob', 'bob@example.com', '2023-09-05 15:45:00'),
 (4, 'alice', 'alice@example.org', '2023-09-06 10:30:00'),
 (5, 'eve', 'eve@gmail.com', '2023-09-07 15:45:00'),
 (4, 'alice', 'alice@gmail.com', '2023-09-08 15:45:00'),
 (5, 'eve', 'eve@example.com', '2023-09-09 15:45:00')
]

# Create a DataFrame from the list of tuples with the defined schema

df=spark.createDataFrame(data,schema=schema)
# Convert update_date column to date type
df.withColumn('update_date',to_timestamp('update_date'))
# Define window specification for dense ranking by id and ordering by update_date
spec=Window.partitionBy("id").orderBy(col("update_date").desc())
# Add a column for the latest email rank using dense ranking
df=df.withColumn('laste_email_id',dense_rank().over(spec))
# Filter the DataFrame to keep only rows with the latest email for each id and drop unnecessary column
final=df.filter(col('laste_email_id')==1).drop('laste_email_id')
# Show the final DataFrame
final.show()

---------------------------------------------->2.fetch the details of employees who are reporting to the same manager<------------------------------------------------

     data = [
 (1,'A',4),
 (2,'B',5),
 (3,'C',6),
 (4,'D',5),
 (5,'E',None),
 (6,'F',None),
 ]
----->output<----
|empid|empname|
+-----+-------+
|    2|      B|
|    4|      D|
+-----+--------
  
schema = ['empid','empname','mgrid']
emp_df = spark.createDataFrame(data,schema)
from pyspark.sql.functions import *
df=emp_df.groupBy('mgrid').agg(count('mgrid').alias('cnt')).filter(col('cnt')>1)
df1=df.join(emp_df,df.mgrid==emp_df.mgrid,'inner')
df1.show()

   ------------------------------->3.second highest salary<--------------------------------


   data = [
    ("John", 1000),
    ("Doe", 2000),
    ("Alice", 3000),
    ("Bob", 4000),
    ("Charlie", 4000) # Duplicate to test deduplication
]

# Creating DataFrame
columns = ["Name", "Salary"]

from pyspark.sql.functions import *
       
df = spark.createDataFrame(data, schema=columns)
df_1=df.select(col('Salary')).distinct()
df_2=df_1.orderBy(col('Salary').desc())
df_2.collect()[1][0]
--------------output:-
 3000

-------------------------------->explations of collect<-----------------------
df_2.collect()
[Row(Salary=4000), Row(Salary=3000), Row(Salary=2000), Row(Salary=1000)]
df_2.collect()[1]
Row(Salary=3000)
df_2.collect()[0]
 Row(Salary=4000)

    
