have 3 files having data first file has 2 column and second has 3 columns and third file has 4 columns but i need to read all the and merge into one file header has column from three files

file1.csv
A,B
1,2
3,4

file2.csv

C,D,E
10,20,30
40,50,60

file3.csv
F,G,H,I
7,8,9,10
11,12,13,14


Output.csv

A,B,C,D,E,F,G,H,I
1,2,10,20,30,7,8,9,10
3,4,40,50,60,11,12,13,14

Senario 1
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df1 = spark.read.csv("file1.csv", header=True)
df2 = spark.read.csv("file2.csv", header=True)
df3 = spark.read.csv("file3.csv", header=True)

# Add row index for joining
from pyspark.sql.functions import monotonically_increasing_id

df1 = df1.withColumn("id", monotonically_increasing_id())
df2 = df2.withColumn("id", monotonically_increasing_id())
df3 = df3.withColumn("id", monotonically_increasing_id())

# Join column-wise based on row number
final = df1.join(df2, "id").join(df3, "id").drop("id")

final.write.csv("merged_output", header=True)


Example Senario-2

file1 has 10 rows, file2 has 8 rows → Python and Spark can handle automatically, but shell cannot.

I can write a script to pad missing rows with NULL if needed.


Below is the cleanest and production-ready PySpark solution to merge 3 files with different number of columns into one final file with combined headers.

This works even if the files have different row counts.

df1 = spark.read.csv("file1.csv", header=True)
df2 = spark.read.csv("file2.csv", header=True)
df3 = spark.read.csv("file3.csv", header=True)

from pyspark.sql.functions import monotonically_increasing_id

df1 = df1.withColumn("id", monotonically_increasing_id())
df2 = df2.withColumn("id", monotonically_increasing_id())
df3 = df3.withColumn("id", monotonically_increasing_id())


merged_df = df1.join(df2, "id", "full") \
               .join(df3, "id", "full") \
               .drop("id")


What this does:

full join ensures:

If row counts mismatch → missing values become NULL

Keeps all columns from all files

Drops helper id column

✅ 👉 Step 4: Write to output
merged_df.write.csv("merged_output", header=True)


