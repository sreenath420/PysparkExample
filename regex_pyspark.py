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
