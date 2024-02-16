------>how to rename mulitple columns at time in spark <-----

def rename_cols(data,old_col,new_col):
  for old_col,new_col in zip(old_col,new_col):
    data=data.withColumnRenamed(old_col,new_col)
  return data

old_col=['Title','Authors','Description','Category','Publisher','Publish Date','Price']
new_col=['Title1','Authors1','Description1','Category1','Publisher1','Publish_Date','Price1']

df_renamed=rename_cols(df_book,old_col,new_col)
df_renamed.show()


+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+
|              Title1|            Authors1|        Description1|           Category1|          Publisher1|        Publish_Date|              Price1|
+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+--------------------+