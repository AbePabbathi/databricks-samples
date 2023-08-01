# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Delta Lake Database Profiler v0.1
# MAGIC 
# MAGIC #### Scope
# MAGIC This notebook analyzes Delta lake tables stored in a database.
# MAGIC It scans the tables and gathers important statistics to help you find tables you need to dig in further
# MAGIC 
# MAGIC Here are the steps we execute
# MAGIC * Read database name entered by user at the top of the notebook
# MAGIC * Gather table stats in using "describe detail" on each table in the database
# MAGIC * Persist the metadata to delta tables in a database called 'dataops'
# MAGIC * Summarize the findings
# MAGIC 
# MAGIC #### Table Statistics
# MAGIC These statistics are captured at table level:
# MAGIC - Table Size in GB
# MAGIC - Avg. File Size
# MAGIC - Partition Columns
# MAGIC 
# MAGIC #### Instructions
# MAGIC 
# MAGIC 1.  Enter the database name in the text box on the top
# MAGIC 2.  Click "Run All" in the menu bar at the top of your browser window

# COMMAND ----------

# Setup Notebook Parameters
# dbutils.widgets.removeAll()
# dbutils.widgets.text("dbname", "abe_demo","DB name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Get Database Name

# COMMAND ----------

# Get the database name from the notebook widget at the top
dbName = getArgument("dbname")

print("This profiler will analyze tables in the database: {}".format(dbName))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Get Table Statistics

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Function to go through each table in the database and gather the statistics
tbl_list = []
# Limit analysis to one database
dbselected = spark.sql("use "+dbName)
tbls = spark.sql("show tables")
# Get a list of all the tables
tbls = tbls.select("database","tableName").filter("database <>''")
tbl_list.extend(tbls.rdd.collect())
tbl_details = []

# COMMAND ----------

# Iterate through each table and get details
while len(tbl_list)>0:
  try:
    # Describe each table to get details
    tableName = tbl_list.pop()[1]
    print("Processing table: "+tableName)
    tblDetails = spark.sql("describe detail "+dbName+"."+tableName )
  except Exception as e:
    pass
                           
  tblDetails=tblDetails.withColumn("dbname",lit(dbName))
  tbl_details.extend(tblDetails.rdd.collect())


# COMMAND ----------

len(tbl_details)

# COMMAND ----------

tSchema = StructType([StructField("format", StringType())\
                      ,StructField("id", StringType())\
                      ,StructField("name", StringType())\
                      ,StructField("description", StringType())\
                      ,StructField("location", StringType())\
                      ,StructField("createdAt", DateType())\
                      ,StructField("lastModified", DateType())\
                      ,StructField("partitionColumns", StringType())\
                      ,StructField("numFiles", IntegerType())\
                      ,StructField("sizeInBytes", LongType())\
                      ,StructField("properties", StringType())\
                      ,StructField("minReaderVersion", StringType())\
                      ,StructField("minWriterVersion", StringType())\
                      ,StructField("dbname", StringType())])
if len(tbl_details)>0:
  tbldetDF = spark.createDataFrame(tbl_details,schema=tSchema)
else :
  tbldetDF = spark.createDataFrame(spark.sparkContext.emptyRDD(), tSchema)

# Create a temporary table for analysis purposes
tbldetDF.createOrReplaceTempView("tables_tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temporary view table_stats
# MAGIC as
# MAGIC select 
# MAGIC dbname,
# MAGIC format,
# MAGIC split(name, '[.]')[1] as table_name,
# MAGIC location,
# MAGIC sizeInBytes,
# MAGIC numFiles,
# MAGIC round(sizeInBytes/1024/1024/1024,3) as sizeInGB,
# MAGIC round(sizeInBytes/1024/1024/numFiles,2) avgFileSizeInMB,
# MAGIC partitionColumns,
# MAGIC case when length(replace(partitionColumns,"[]",""))>0 then 1 else 0 end as partitionFlag 
# MAGIC from tables_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table_stats

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Persist the table metadata to a database called 'dataops'

# COMMAND ----------

#%sql
#create database if not exists dataops

# COMMAND ----------

#%sql
#drop table if exists dataops.tbl_details

# COMMAND ----------

#%sql
#create table if not exists dataops.tbl_details as select * from table_stats

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If table already exists
# MAGIC merge into dataops.tbl_details as t
# MAGIC using 
# MAGIC table_stats as s
# MAGIC on t.dbname = s.dbname and t.table_name = s.table_name
# MAGIC when matched then update set *
# MAGIC when not matched then insert *;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Summarize Statistics

# COMMAND ----------

# DBTITLE 1,Top 10 Tables
# MAGIC %sql
# MAGIC -- Top 10 tables
# MAGIC select 
# MAGIC dbname,table_name,sizeInGB,partitionFlag
# MAGIC from dataops.tbl_details 
# MAGIC order by sizeInGB desc
# MAGIC limit 10
