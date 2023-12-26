# Databricks notebook source
# DBTITLE 1,Test connection to Azure service
#dbutils.fs.mount(
# source='wasbs://bronze@adestorageaccounts.blob.core.windows.net/',
# mount_point = '/mnt/bronze',
# extra_configs = {'fs.azure.account.key.adestorageaccounts.blob.core.windows.net': dbutils.secrets.get('databricksScope3','storageAccountKey')}
#)

# COMMAND ----------

#dbutils.fs.ls('/mnt/bronze/20231226')

# COMMAND ----------

fileName = dbutils.widgets.get('fileName')
tableSchema = dbutils.widgets.get('table_schema')
tableName = dbutils.widgets.get('table_name')
print(f"\nfile name ", fileName) 
print(f"\ntable name ", tableName) 
print(f"\nschema name ", tableSchema)

spark.sql(f'create database if not exists {tableSchema}')
spark.sql("""CREATE TABLE IF NOT EXISTS """+tableSchema+"""."""+tableName+"""
            USING PARQUET
            LOCATION '/mnt/bronze/"""+fileName+"""/"""+tableSchema+"""."""+tableName+""".parquet'
          """)

# COMMAND ----------

display(spark.sql("select * from saleslt.customer"))
