from pyspark.sql.functions import split
#reading csv file from source
df=spark.read.option("header","true").option("sep",",").csv("dbfs:/FileStore/muliti_delimiter.csv")

df.show()


+---+----+---+--------+
| ID|Name|Age|   Marks|
+---+----+---+--------+
|  1|   A| 20|31|32|34|
|  2|   B| 21|21|32|43|
|  3|   C| 22|21|32|11|
|  4|   D| 23|10|11|12|
+---+----+---+--------+

#split thte delimiter 
df1 = df.withColumn("maths",split("Marks","\\|")[0]) \
        .withColumn("physics",split("Marks","\\|")[1]) \
        .withColumn("chemsitry",split("Marks","\\|")[2]) \
        .drop("Marks")
df1.show()


+---+----+---+-----+-------+---------+
| ID|Name|Age|maths|physics|chemsitry|
+---+----+---+-----+-------+---------+
|  1|   A| 20|   31|     32|       34|
|  2|   B| 21|   21|     32|       43|
|  3|   C| 22|   21|     32|       11|
|  4|   D| 23|   10|     11|       12|
+---+----+---+-----+-------+---------+