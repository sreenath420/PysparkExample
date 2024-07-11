The regexp_extract function in PySpark is useful for extracting specific patterns from a string column using regular expressions. 
Here's a detailed explanation and example of how to use regexp_extract in PySpark


---------------------------->convert string values to numeric in pyspark<------------------------------------------
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

------------------------------>second problem<---------------------


  from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract


input

+--------+
|    text|
+--------+
|  1000abc|
|  xyz2000|
|  3000def|
|  4000ghi|
+--------+
output
+--------+------------+---------------+
|    text|numeric_part|alphabetic_part|
+--------+------------+---------------+
|  1000abc|        1000|            abc|
|  xyz2000|        2000|            xyz|
|  3000def|        3000|            def|
|  4000ghi|        4000|            ghi|
+--------+------------+---------------+


# Create Spark session
spark = SparkSession.builder \
    .appName("RegexpExtractExample") \
    .getOrCreate()

# Sample data
data = [("1000abc",), ("xyz2000",), ("3000def",), ("4000ghi",)]
columns = ["text"]

# Create DataFrame
df = spark.createDataFrame(data, columns)
df.show()

# Extract numeric values
df = df.withColumn("numeric_part", regexp_extract("text", "([0-9]+)", 1))

# Extract alphabetic values
df = df.withColumn("alphabetic_part", regexp_extract("text", "([a-zA-Z]+)", 1))

df.show()


