better way to optimize below code

from pyspark.sql import SparkSession

from pyspark.sql.functions import year
 
spark = SparkSession.builder.appName("UnoptimizedSales").getOrCreate()
 

orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)


customers_df = spark.read.csv("customers.csv", header=True, inferSchema=True)

products_df = spark.read.csv("products.csv", header=True, inferSchema=True)
 

joined_df = orders_df.join(customers_df, "customer_id", "inner")

full_join_df = joined_df.join(products_df, "product_id", "inner")
 
 
filtered_df = full_join_df.filter(

    (full_join_df['category'] == 'Electronics') & (year(full_join_df['order_date']) == 2024)

)
 
result = filtered_df.groupBy('customer_id', 'customer_name').sum('sale_price')


print(result.collect())
 ---------------------------------------------------------------Answer better performance<-----------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, col, sum as _sum

spark = SparkSession.builder.appName("OptimizedSales").getOrCreate()

# 1️⃣ Define schema instead of inferSchema (BIG performance improvement)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("order_date", DateType(), True),
    StructField("sale_price", DoubleType(), True)
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("category", StringType(), True)
])

orders_df = spark.read.csv("orders.csv", header=True, schema=orders_schema)
customers_df = spark.read.csv("customers.csv", header=True, schema=customers_schema)
products_df = spark.read.csv("products.csv", header=True, schema=products_schema)

# 2️⃣ Filter early → reduces join size massively
orders_filtered = orders_df.filter(year(col("order_date")) == 2024)

products_filtered = products_df.filter(col("category") == "Electronics")

# 3️⃣ Broadcast small tables (customers/products usually small)
from pyspark.sql.functions import broadcast

joined_df = (
    orders_filtered
    .join(broadcast(customers_df), "customer_id")
    .join(broadcast(products_filtered), "product_id")
)

# 4️⃣ Final aggregation
result = (
    joined_df
    .groupBy("customer_id", "customer_name")
    .agg(_sum("sale_price").alias("total_sales"))
)

result.show()


result.show()
 twitter --->brozer layer (raw data)---> silver 
 
