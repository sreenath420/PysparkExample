Topic----->Create DataFrame with schema

Insert vaalues---------------------------------------------------------------------------------------------------
Student_id, student_name, dept_id
1, abc, 1
2, def, 1
3, ghi, 2
4, jkl, 1

from pyspark.sql.types import StructType,StructField, StringType, IntegerType
data=[(1,"abc",1),
      (2,"def",1),
	  (3,"ghi",2),
	  (4,"jkl",1)]
	  
schema = StructType([
       StructField("Student_id",IntegerType(),True), \
	   StructField("student_name",StringType(),True), \
	   StructField("dept_id",IntegerType(),True)])
	   
	   
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()


data2 =[(1,"DP1"),
       (2,"DP2")]
	   
schema1= StructType([
         Structfield("Dept_id",IntegerType(),True),\
		 Structfield("dept_name",StringType(),True)])
		 
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()

		 
Student_id, student_name, dept_id, dept_name		 
1, abc, 1, DP1
2, def, 1, DP1
4, jkl, 1, DP1
3, ghi, 2, DP2

data2 =[(1,"DP1"),
       (2,"DP2")]
	   
schema1= StructType([
         StructField("Dept_id",IntegerType(),True),\
		     StructField("dept_name",StringType(),True)])
			 
df1 = spark.createDataFrame(data=data2,schema=schema1)
df1.show()			 
    
Dept_id, dept_name
1, DP1
2, DP2

df.createOrReplaceTempView("student")
df1.createOrReplaceTempView("department")

df_student=spark.sql("select s.*,d.dept_name from student s join department d on s.dept_id=d.Dept_id")
df_student.show()

df.join(df1,df.dept_id==df1.Dept_id,"inner")\
  .show()
 
+----------+------------+-------+-------+---------+
|Student_id|student_name|dept_id|Dept_id|dept_name|
+----------+------------+-------+-------+---------+
|         1|         abc|      1|      1|      DP1|
|         2|         def|      1|      1|      DP1|
|         4|         jkl|      1|      1|      DP1|
|         3|         ghi|      2|      2|      DP2|
+----------+------------+-------+-------+---------+
 
from pyspark.sql.functions import col
df4=df.join(df1,df.dept_id==df1.Dept_id,"inner")
df4.select(df.Student_id,df.student_name,df1.Dept_id,df1.dept_name).show()

+----------+------------+-------+---------+
|Student_id|student_name|Dept_id|dept_name|
+----------+------------+-------+---------+
|         1|         abc|      1|      DP1|
|         2|         def|      1|      DP1|
|         4|         jkl|      1|      DP1|
|         3|         ghi|      2|      DP2|
+----------+------------+-------+---------+
