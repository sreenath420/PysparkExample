------>how to rename mulitple columns at time in spark <-----

# Example: Create df_book DataFrame
data = [
  ['Book A', 'Author X', 'Desc 1', 'Fiction', 'Pub1', '2021-01-01', 10.99],
  ['Book B', 'Author Y', 'Desc 2', 'Non-Fiction', 'Pub2', '2022-02-02', 12.99]
]
columns = [
  'Title', 'Authors', 'Description', 'Category', 'Publisher', 'Publish Date', 'Price'
]
df_book = spark.createDataFrame(data, columns)

def rename_cols(data, old_col, new_col):
  for old, new in zip(old_col, new_col):
    data = data.withColumnRenamed(old, new)
  return data

old_col = [
  'Title', 'Authors', 'Description', 'Category', 'Publisher', 'Publish Date', 'Price'
]
new_col = [
  'Title1', 'Authors1', 'Description1', 'Category1', 'Publisher1', 'Publish_Date', 'Price1'
]

df_renamed = rename_cols(df_book, old_col, new_col)
display(df_renamed)


output:-
Title1	Authors1	Description1	Category1	Publisher1	Publish_Date	Price1
Book A	Author X	Desc 1	Fiction	Pub1	2021-01-01	10.99
Book B	Author Y	Desc 2	Non-Fiction	Pub2	2022-02-02	12.99
