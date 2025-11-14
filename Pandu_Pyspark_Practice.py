------------------------------------------------------->>>>Queation-1<<<-----------------------------------------------
1.How would you add a new column discount with values 0	
if amount is less than 200, 10 if
amount is between 200 and 1000, and 20 if amount is greater than 1000?


A DataFrame sales with columns sale_id, amount.
val sales = List(
 (1, 100),
 (2, 1500),
 (3, 300)
).toDF("sale_id", "amount")

from pyspark.sql import SparkSession
from pyspark.sql.functions  import when 
df = spark.createDataFrame([(1, 100), (2, 1500), (3,300)], ["sales_id", "amount"]) 
df=df.withColumn( "Discount",
                 when (df.amount<200,"0").\
                     when (df.amount.between(200,1000),"10").\
                        otherwise('30'))

df.show()

