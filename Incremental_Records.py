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



--------------->insert new_records and update existing records and remove dupicateds and handing null values<-----------


Step 1: Create Data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SimpleUpsertPractice").getOrCreate()

# Target data
target_data = [
    (1, "Alice", "HR", 50000),
    (2, "Bob", "IT", 60000)
]

# Source data (duplicate id=2)
source_data = [
    (2, "Bob", "IT", 62000),
    (2, "Bob", "IT", 65000),  # latest
    (3, "Charlie", "Finance", 70000)
]

columns = ["id", "name", "department", "salary"]

target_df = spark.createDataFrame(target_data, columns)
source_df = spark.createDataFrame(source_data, columns)

print("Target Table")
target_df.show()

print("Source Table (With Duplicates)")
source_df.show()


Step 2: Handle Duplicate IDs in Source

Window=Window.partitionBy("id").orderBy(col("salary").desc())
source_df=source_df.withColumn("rnk",row_number().over(Window)).\
    filter(col("rnk")==1).drop("rnk")
    
 Step 3: Find Updates
updates=source_df.alias("s").join(target_df.alias("t"),on="id",how="inner")
updates=updates.selectExpr("s.id","coalesce(s.name,t.name) as name","coalesce(s.department,t.department) as department","coalesce(s.salary,t.salary) as salary")

updates.show()

Step 4: Find Inserts 
new_records=source_df.join(target_df,on="id",how="leftanti")
new_records.show()

Step 5: Remove Old Records from Target
old_records=target_df.join(updates,on="id",how="leftanti")
old_records.show()

Step 6: Final Upsert Result
merged_df=old_records.union(updates).union(new_records)
merged_df.show()



----->How Incremental Solves this problem why we need to do this<---------------------------


updated records by incrementally

Why Watermark Is Powerful

Without watermark:

Scan full 10M source

Join full target

Heavy shuffle

With watermark:

Scan only new rows

Join only few IDs

Minimal shuffle


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from datetime import datetime

spark = SparkSession.builder.appName("IncrementalStepByStep").getOrCreate()

target_data = [
    (1, "Alice", 50000, datetime(2024,1,10,10,0)),
    (2, "Bob", 60000, datetime(2024,1,12,12,0))
]

columns = ["id", "name", "salary", "last_updated"]

target_df = spark.createDataFrame(target_data, columns)

print("Existing Target Data")
target_df.show(truncate=False)


source_data = [
    (1, "Alice", 50000, datetime(2024,1,10,10,0)),  # old
    (2, "Bob", 65000, datetime(2024,1,18,15,0)),    # updated
    (3, "Charlie", 70000, datetime(2024,1,20,9,0))  # new
]

source_df = spark.createDataFrame(source_data, columns)

print("Incoming Source Data")
source_df.show(truncate=False)

----> here the condition we make update date for implement that logic<-------------------------------
last_updated_ts=datetime(2024,1,15,0,0)
print("Last Processed WaterMark",last_updated_ts)



incremental_df=source_df.filter(col("last_updated")>last_updated_ts)
incremental_df.show()


updated_records=incremental_df.alias("s").join(target_df.alias("t"),on='id',how='inner').filter(col("s.salary") != col("t.salary")).select("s.*")
updated_records.show()

new_records=incremental_df.join(target_df,on='id',how='leftanti')
new_records.show()

existing_records=target_df.join(incremental_df,on='id',how='leftanti')
existing_records.show()


final_df=existing_records.unionByName(incremental_df).unionByName(updated_records)
final_df.show()
