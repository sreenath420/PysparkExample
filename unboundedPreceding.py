In PySpark, the unboundedPreceding function is used to define a window frame that includes all rows from the start of the partition up to the current row. This is typically used in window functions to perform calculations over a specified range of rows relative to the current row.

It defines dynamic windows that depend on the row on which a calculation is being performed.

currentRow- indicates the current row
unboundedPreceding- indicates the first row of the group.
unboundedFollowing- indicates the last row of the group

data = [
    ("A", 1),
    ("A", 2),
    ("A", 3),
    ("B", 1),
    ("B", 2),
    ("B", 3)
]

columns = ["Category", "Value"]

df = spark.createDataFrame(data, schema=columns)


from pyspark.sql.window import *
from pyspark.sql.functions import *
windowSpec = Window.partitionBy("Category").orderBy("Value").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("CumulativeSum", sum(col("Value")).over(windowSpec)).show()


+--------+-----+-------------+
|Category|Value|CumulativeSum|
+--------+-----+-------------+
|       A|    1|            1|
|       A|    2|            3|
|       A|    3|            6|
|       B|    1|            1|
|       B|    2|            3|
|       B|    3|            6|



