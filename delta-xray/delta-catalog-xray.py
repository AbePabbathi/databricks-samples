# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Delta Lake Catalog Profiler v0.1
# MAGIC 
# MAGIC #### Scope
# MAGIC This notebook analyzes databases and tables stored in a catalog.
# MAGIC 
# MAGIC Here are the steps we execute
# MAGIC * Read database name entered by user at the top of the notebook
# MAGIC * Gather table names in each database
# MAGIC * Persist the table names in a database called 'dataops'
# MAGIC * Summarize the findings
# MAGIC 
# MAGIC #### Database Statistics
# MAGIC These statistics are captured at database level:
# MAGIC - No. of tables
# MAGIC 
# MAGIC 
# MAGIC #### Instructions
# MAGIC 
# MAGIC 1.  Enter the catalog name in the text box on the top
# MAGIC 2.  Click "Run All" in the menu bar at the top of your browser window

# COMMAND ----------

# Setup Notebook Parameters
#dbutils.widgets.removeAll()
#dbutils.widgets.text("catalogName", "hive_metastore","Catalog Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Get Database Name

# COMMAND ----------

# Get the database name from the notebook widget at the top
catalogName = getArgument("catalogName")

print("This profiler will analyze databases in the catalog: {}".format(catalogName))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Get Table Statistics

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Get all databases
catSelected = spark.sql("use "+catalogName)
databases = spark.sql("show databases")
databases = databases.withColumn("catalogName",lit(catalogName))
db_list = list(databases.select("databaseName").rdd.collect())
databases.createOrReplaceTempView("dbtmp")

# COMMAND ----------

# Function to go through each table in the database and gather the statistics
def get_tbls(dbname):
  tbl_list = []
  # Limit analysis to one database
  dbselected = spark.sql("use "+dbname)
  tbls = spark.sql("show tables")
  # Get a list of all the tables
  tbls = tbls.select("database","tableName").filter("database <>''")
  tbl_list.extend(tbls.rdd.collect())
  return tbl_list

# COMMAND ----------

tables = []

while len(db_list)>0:
  dbname = db_list.pop()[0]
  tbl_list = get_tbls(dbname)
  tables.extend(tbl_list)

# COMMAND ----------

tSchema = StructType([StructField("database", StringType())\
                      ,StructField("tableName", StringType())])
if len(tables)>0:
  tblDF = spark.createDataFrame(tables,schema=tSchema)
else :
  tblDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), tSchema)

# Create a temporary table for analysis purposes
tblDF.createOrReplaceTempView("tables_tmp")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Persist the table metadata to a database called 'dataops'

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists dataops

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists dataops.db_details;
# MAGIC create table if not exists dataops.db_details as select catalogName,databaseName,0 as processedFlag from dbtmp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If table already exists
# MAGIC merge into dataops.db_details as t
# MAGIC using 
# MAGIC (select catalogName,databaseName,0 as processedFlag from dbtmp) as s
# MAGIC on t.catalogName = s.catalogName and t.databaseName = s.databaseName
# MAGIC when matched then update set *
# MAGIC when not matched then insert *;

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists dataops.db_tables;
# MAGIC create table if not exists dataops.db_tables as select * from tables_tmp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If table already exists
# MAGIC merge into dataops.db_tables as t
# MAGIC using 
# MAGIC tables_tmp as s
# MAGIC on t.database = s.database and t.tableName = s.tableName
# MAGIC when matched then update set *
# MAGIC when not matched then insert *;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Summarize Statistics

# COMMAND ----------

# DBTITLE 1,Top Databases by Table Count
# MAGIC %sql
# MAGIC -- Top databases
# MAGIC select 
# MAGIC database,count(*) table_count
# MAGIC from dataops.db_tables 
# MAGIC group by database order by 2 desc
