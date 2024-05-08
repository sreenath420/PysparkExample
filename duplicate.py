input-

+---+-------+
| id|  email|
+---+-------+
|  1|a@b.com|
|  2|c@d.com|
|  3|a@b.com|
+---+-------+

output-
+-------+
|  email|
+-------+
|a@b.com|
+-------+



from pyspark.sql.types import StructType,StructField, StringType, IntegerType
emails_data=[
(1, 'a@b.com'),
(2, 'c@d.com'),
(3, 'a@b.com')]

emails_schema = StructType([
StructField("id", IntegerType(), True),\
StructField("email", StringType(), True)])

df = spark.createDataFrame(data=emails_data,schema=emails_schema)
df.printSchema()

--------------------------------spar.sql------------------------------------------------------
df1=spark.sql("""select email from df_view group by email having count(email)>1 """)
df1.show()

--------------------------------pyspark------------------------------------------------------
from pyspark.sql.functions import *
df2=df.groupby("email").count()
df2.select("email").filter(col("count") > 1).show()
#df2.filter("count > 1").drop("count").show()
#df2.drop("count").show()