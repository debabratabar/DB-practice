# Databricks notebook source
# MAGIC %md
# MAGIC ### Read and write using python 

# COMMAND ----------

#storage account details
storage_account_name="synapsestorage1st"
container_name="spark-data"
storage_account_access_key="WLs88CucPJ0C+vWwF1Hg3TeXuYgdG2puMHta/q9KL1G1Q6qVAa2Cdt5/l//IPgBYMkAw237IedWi+AStQ4AmuQ=="
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
filepath="wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net/email_data.csv/"

# COMMAND ----------

#reading file from blob
file_df= spark.read.csv(filepath  , header="true" , inferSchema = "true", escape='"')

# COMMAND ----------

#printing the dataframe
file_df.show()

# COMMAND ----------

#creating a hive managed table   in databricks from this csv file 
file_df.write.saveAsTable('email_data_managed_table')

# COMMAND ----------

# MAGIC %md
# MAGIC N0te that the table 'email_data_managed_table' will be stored in default location "dbfs:user/hive/warehouse/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select attributes from email_data_managed_table 

# COMMAND ----------

# MAGIC %sql
# MAGIC --to check the table details
# MAGIC describe extended email_data_managed_table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read and write using sql 

# COMMAND ----------

#storage account details
storage_account_name="synapsestorage1st"
container_name="spark-data"
storage_account_access_key="WLs88CucPJ0C+vWwF1Hg3TeXuYgdG2puMHta/q9KL1G1Q6qVAa2Cdt5/l//IPgBYMkAw237IedWi+AStQ4AmuQ=="
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
filepath="wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net/email_data.csv/"

# COMMAND ----------

print(filepath)

# COMMAND ----------

#after setting the filepath we have to configure the path with a new name

spark.conf.set("source-file-path", filepath)


# COMMAND ----------

# MAGIC %sql
# MAGIC -- first create a schema/ database 
# MAGIC create schema if not exists database_practice 

# COMMAND ----------

# MAGIC %sql
# MAGIC select "${source-file-path}" as file_path

# COMMAND ----------

# MAGIC %sql
# MAGIC -- first creating a view to create a table , 
# MAGIC create or replace temporary view temp_email_data
# MAGIC using csv 
# MAGIC options(
# MAGIC path="${source-file-path}",
# MAGIC header= "true",
# MAGIC inferschema= "true",
# MAGIC delimeter=',',
# MAGIC escape='"');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- creating a table from a temporary view
# MAGIC create or replace  table  email_data_using_sql
# MAGIC As
# MAGIC select * from temp_email_data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- checking the data
# MAGIC select * from email_data_using_sql 

# COMMAND ----------


