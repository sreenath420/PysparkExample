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


---->insert new_records along with updated records<-------------------------

✅ Insert new records

✅ Update existing records

Step 1: Create Sample Data

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleIncremental").getOrCreate()

# Target data
target_data = [
    (1, "Alice", 50000),
    (2, "Bob", 60000)
]

# Source data
source_data = [
    (2, "Bob", 65000),   # Updated
    (3, "Charlie", 70000)  # New
]

columns = ["id", "name", "salary"]

target_df = spark.createDataFrame(target_data, columns)
source_df = spark.createDataFrame(source_data, columns)

print("Target Table")
target_df.show()

print("Source Table")
source_df.show()

Step 2: Separate Updates and Inserts
⃣ Find Updates
updates = source_df.alias("s").join(
    target_df.alias("t"),
    on="id"
).select("s.*")

updates.show()

-----Find Inserts

inserts = source_df.join(
    target_df,
    on="id",
    how="left_anti"
)

inserts.show()

Step 3: Remove Old Records That Need Update


target_filtered = target_df.join(
    updates,
    on="id",
    how="left_anti"
)

target_filtered.show()

final_df = target_filtered.union(updates).union(inserts)

print("Final Table After Upsert")
final_df.show()


