
You are given a dataframe having columns Name and Gender. The Gender value given in the dataframe is not correct. Your task is to correctly put the gender value.

from pyspark.sql import Row
data = [
    Row(Name='Ram', Gender='Female'),
    Row(Name='Ajay', Gender='Female'),
    Row(Name='Meena', Gender='Male'),
    Row(Name='Neha', Gender='Male')
]

df = spark.createDataFrame(data)

from pyspark.sql.functions import *
df_transformed = df.withColumn(
    "Gender",
    when(col("Gender") == "Male", "Female")
    .when(col("Gender") == "Female", "Male")
    .otherwise(col("Gender"))
)
df_transformed.show()

output:-

+-----+------+
| Name|Gender|
+-----+------+
|  Ram|  Male|
| Ajay|  Male|
|Meena|Female|
| Neha|Female|
+-----+------+