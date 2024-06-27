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