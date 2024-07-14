+----+--------+----+------+
|e_id|emp_name|dept|salary|
+----+--------+----+------+
|   1|  Mahesh|  IT| 22000|
|   2|   Payal|  HR| 13000|
|   3|  Satish|  IT| 25000|
|   4|    Amit|  IT| 24000|
|   5|  Swetha|  HR| 14000|
|   6|   Minal|  IT| 23000|
+----+--------+----+------+


+----+--------+----+------+----+--------------+
|e_id|emp_name|dept|salary|dept|average_salary|
+----+--------+----+------+----+--------------+
|   3|  Satish|  IT| 25000|  IT|       23500.0|
|   4|    Amit|  IT| 24000|  IT|       23500.0|
|   5|  Swetha|  HR| 14000|  HR|       13500.0|
+----+--------+----+------+----+--------------+



--------------------------------------------------------------------------------->Dataframe<-----------------------------------------------

from pyspark.sql.types import StructField,StructType,IntegerType,StringType
data=[(1,"Mahesh","IT",22000),
      (2,"Payal","HR",13000),
	  (3,"Satish","IT",25000),
	  (4,"Amit","IT",24000),
	  (5,"Swetha","HR",14000),
	  (6,"Minal","IT",23000)]
schema=StructType([StructField("e_id",IntegerType(),True),
                  StructField("emp_name",StringType(),True),
				          StructField("dept",StringType(),True),
				          StructField("salary",IntegerType(),True)])	


from pyspark.sql.functions import *
df1=df.groupBy("dept").agg(avg("salary").alias("average_salary"))
##df1.show()
df_result=df.join(df1,df.dept==df1.dept,"inner")
df_result.show()
df2=df_result.filter(df.salary > df1.average_salary)
df2.show()


------------------------------------------------------------>DataFrame<-------------------------------------------------------------

df1 = spark.createDataFrame([
 (1, "Smith", 2018, 10, "M", 3000),
 (2, "Rose", 2010, 20, "M", 4000),
 (3, "Williams", 2010, 20, "M", 1000),
 (4, "Jones", 2005, 10, "F", 2000),
 (5, "Brown", 2010, 40, None, -1),
 (6, "Brown", 2010, 50, None, -1)
], ["emp_id", "name", "year_joined", "emp_dept_id", "gender", "salary"])

df2 = spark.createDataFrame([
 ("Finance", 10),
 ("Marketing", 20),
 ("Sales", 30),
 ("IT", 40)], ["dept_name", "dept_id"])

# Q1: Get name and dept_name from these 2 tables
joined_df = df1.join(df2, df1.emp_dept_id == df2.dept_id, "inner")
result_q1 = joined_df.select("name", "dept_name")
result_q1.show()
+--------+---------+
|    name|dept_name|
+--------+---------+
|   Smith|  Finance|
|   Jones|  Finance|
|    Rose|Marketing|
|Williams|Marketing|
|   Brown|       IT|
+--------+---------+

# Q2: Get highest salary from each dept

from pyspark.sql.functions import *
df_max=joined_df.groupBy('dept_name').agg(max('salary'))
df_max.show()

+---------+-----------+
|dept_name|max(salary)|
+---------+-----------+
|  Finance|       3000|
|Marketing|       4000|
|       IT|         -1|
+---------+-----------+

# Q3: Get a new column gender_full

df1_with_gender_full=df1.withColumn("gender_full", 
 when(col("gender") == "M", "Male")
 .when(col("gender") == "F", "Female")
 .otherwise("Unknown"))

df1_with_gender_full.show()

+------+--------+-----------+-----------+------+------+-----------+
|emp_id|    name|year_joined|emp_dept_id|gender|salary|gender_full|
+------+--------+-----------+-----------+------+------+-----------+
|     1|   Smith|       2018|         10|     M|  3000|       Male|
|     2|    Rose|       2010|         20|     M|  4000|       Male|
|     3|Williams|       2010|         20|     M|  1000|       Male|
|     4|   Jones|       2005|         10|     F|  2000|     Female|
|     5|   Brown|       2010|         40|  null|    -1|    Unknown|
|     6|   Brown|       2010|         50|  null|    -1|    Unknown|
+------+--------+-----------+-----------+------+------+-----------+

