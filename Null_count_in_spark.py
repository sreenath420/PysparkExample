------->we have multiple nulls values in 20 columns how to count those values by using spark?<--------


df_book = spark.read.option("sep",",").option("header","true").csv("dbfs:/FileStore/BooksDataset.csv")

from pyspark.sql.functions import col, sum
null_counts = df_book.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_book.columns])
null_counts.show()


+-----+-------+-----------+--------+---------+------------+-----+
|Title|Authors|Description|Category|Publisher|Publish Date|Price|
+-----+-------+-----------+--------+---------+------------+-----+
|    0|      0|      32851|   25763|       82|          59|   41|
+-----+-------+-----------+--------+---------+------------+-----+
