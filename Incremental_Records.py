----->Insert only new_records even there is update records only insert new records<-------------------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IncrementalInsert").getOrCreate()

# Existing target data
target_data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Charlie")
]

target_df = spark.createDataFrame(target_data, ["id", "name"])

target_df.show()


source_data = [
    (2, "Bob"),        # already exists
    (3, "Charlie"),    # already exists
    (4, "David"),      # new
    (5, "Emma")        # new
]

source_df = spark.createDataFrame(source_data, ["id", "name"])

source_df.show()

df_insert=source_df.join(target_df,on='id',how='leftanti')
--->Here leftanti join only gives us non mathing records from righttable <--------------
df_insert.show()

df_update=df_insert.union(target_df)


df_update.show()
