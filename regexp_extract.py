-------------------------------------------------------------------->regexp_extract<----------------------------------------------------------------

regexp_extract in pyspark used to extract a specific matching group from a string column based on a regular expression pattern. This function is particularly useful when you need to extract specific parts of a string, such as dates,numbers, or any other pattern within string column.

Syntax:

regexp_extract(column: Column, pattern: str, idx: int)

column: The column from which to extract data.
pattern: The regular expression pattern.
idx: The index of the capturing group in the regex pattern to extract. The index is 1-based; idx=1 corresponds to the first group, idx=2 to the second, etc.

from pyspark.sql.functions import regexp_extract
data = [("2024-08-22",), ("1990-12-31",), ("2000-01-01",)]
df = spark.createDataFrame(data, ["date"])

df=df.withColumn('year',regexp_extract(df['date'],r"(\d{4})-\d{2}-\d{2}", 1))

df.show()

+----------+----+
|      date|year|
+----------+----+
|2024-08-22|2024|
|1990-12-31|1990|
|2000-01-01|2000|
+----------+----+
------------------>detais of idx in regexp_extract<----------------


The idx parameter in the regexp_extract function determines which capturing group from the regular expression you want to extract. The capturing groups are denoted by parentheses () in the regex pattern. The idx is 1-based, meaning:

idx=1 corresponds to the first capturing group (the first set of parentheses in the pattern).
idx=2 corresponds to the second capturing group, and so on.

data = [("2024-08-22 15:30:00",), ("1990-12-31 23:59:59",), ("2000-01-01 00:00:00",)]
df = spark.createDataFrame(data, ["datetime"])
df=df.withColumn('value',regexp_extract(df['datetime'],r"(\d{4})-(\d{2})-(\d{2})", 2))

df.show()

+-------------------+-----+
|           datetime|value|
+-------------------+-----+
|2024-08-22 15:30:00|   08|
|1990-12-31 23:59:59|   12|
|2000-01-01 00:00:00|   01|
+-------------------+-----+


2. Extracting Area Code from Phone Numbers


data = [("(123) 456-7890",), ("(987) 654-3210",), ("(555) 123-4567",)]
df = spark.createDataFrame(data, ["phone_number"])

# Extract the area code
df = df.withColumn("area_code", regexp_extract(df["phone_number"], r"\((\d{3})\)", 1))

df.show()

+--------------+---------+
|  phone_number|area_code|
+--------------+---------+
|(123) 456-7890|      123|
|(987) 654-3210|      987|
|(555) 123-4567|      555|
+--------------+---------+


3.Extracting Domain Name from Email Addresses

data = [("user1@gmail.com",), ("user2@yahoo.com",), ("user3@outlook.com",)]
df = spark.createDataFrame(data, ["email"])

# Extract the domain name
df = df.withColumn("domain", regexp_extract(df["email"], r"@([a-zA-Z0-9.-]+)", 1))

df.show()

+-----------------+-----------+
|            email|     domain|
+-----------------+-----------+
|  user1@gmail.com|  gmail.com|
|  user2@yahoo.com|  yahoo.com|
|user3@outlook.com|outlook.com|
+-----------------+-----------+


4. Extracting Price from a String

# Sample DataFrame
data = [("Price: $123.45",), ("Total cost: $99.99",), ("Amount: $1000.00",)]
df = spark.createDataFrame(data, ["description"])

# Extract the price
df = df.withColumn("price", regexp_extract(df["description"], r"\$(\d+\.\d{2})", 1))

df.show()


+------------------+-------+
|       description|  price|
+------------------+-------+
|    Price: $123.45| 123.45|
|Total cost: $99.99|  99.99|
|  Amount: $1000.00|1000.00|
+------------------+-------+

-------------------------------------------------------->regex_replace<--------------------------------------------------------------------------------------


In PySpark, regexp_replace is a built-in function that allows you to replace substrings in a string column that match a regular expression with a new substring. This is useful for data cleaning and transformation tasks where you need to standardize or modify string data.

from pyspark.sql.functions import regexp_replace

regexp_replace(column, pattern, replacement)

column: The column on which the regex replacement is to be performed.
pattern: The regular expression pattern to search for in the column.
replacement: The string to replace the matched patterns with.

from pyspark.sql.functions import regexp_replace
data = [("Hello, World!",), ("This is #$% a test.",), ("Clean up @@@ these!!!",)]
df = spark.createDataFrame(data, ["text"])
df=df.withColumn('text',regexp_replace(df['text'],r"[^a-zA-Z0-9\s]", ""))

df.show()

+---------------+
|           text|
+---------------+
|    Hello World|
|This is  a test|
|Clean up  these|
+---------------+


2. Standardizing Phone Numbers

# Sample DataFrame
data = [("123-456-7890",), ("(123) 456 7890",), ("123.456.7890",)]
df = spark.createDataFrame(data, ["phone_number"])


# Step 1: Remove all non-numeric characters
df = df.withColumn("cleaned_number", regexp_replace(df["phone_number"], r"[^0-9]", ""))

# Step 2: Format the cleaned number as XXX-XXX-XXXX
df = df.withColumn("standardized_number", regexp_replace(df["cleaned_number"], r"(\d{3})(\d{3})(\d{4})", r"\1-\2-\3"))

df.show(truncate=False)

+--------------+-------------+-------------------+
|phone_number  |cleaned_number|standardized_number|
+--------------+-------------+-------------------+
|123-456-7890  |1234567890    |123-456-7890       |
|(123) 456 7890|1234567890    |123-456-7890       |
|123.456.7890  |1234567890    |123-456-7890       |
+--------------+-------------+-------------------+

3. Masking Sensitive Data


# Sample DataFrame
data = [("1234-5678-9101-1121",), ("9876-5432-1098-7654",), ("1111-2222-3333-4444",)]
df = spark.createDataFrame(data, ["credit_card"])


# Replace all digits except the last four with asterisks
df = df.withColumn("masked_credit_card", regexp_replace(df["credit_card"], r"\d(?=\d{4})", "*"))

df.show(truncate=False)


+--------------------+-------------------+
|credit_card         |masked_credit_card  |
+--------------------+-------------------+
|1234-5678-9101-1121 |****-****-****-1121 |
|9876-5432-1098-7654 |****-****-****-7654 |
|1111-2222-3333-4444 |****-****-****-4444 |
+--------------------+-------------------+


