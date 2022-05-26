# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Delta Lake Database Profiler v0.1
# MAGIC 
# MAGIC #### Scope
# MAGIC This notebook analyzes Delta lake tables stored in a database.
# MAGIC It scans the tables and dbfs file system to gather important statistics to help you find ways to improve the performance of Delta lake tables
# MAGIC 
# MAGIC Here are the steps we execute
# MAGIC * Read database name entered by user at the top of the notebook
# MAGIC * Gather table stats in using "describe detail" on each table in the database
# MAGIC * Gather file level details using the location of each table
# MAGIC * Persist the metadata to delta tables in a database called 'dataops'
# MAGIC * Summarize the findings
# MAGIC 
# MAGIC #### Table Statistics
# MAGIC These statistics are captured at table level:
# MAGIC - Table Size in GB
# MAGIC - Avg. File Size
# MAGIC 
# MAGIC #### DBFS File Statistics
# MAGIC These statistics are captured at dbfs file level:
# MAGIC - Table Size including all versions
# MAGIC - Avg. File Size
# MAGIC - Partition Skew
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
def get_tbls_dets(dbname):
  tbl_list = []
  # Limit analysis to one database
  dbselected = spark.sql("use "+dbname)
  tbls = spark.sql("show tables")
  # Get a list of all the tables
  tbls = tbls.select("database","tableName").filter("database <>''")
  tbl_list.extend(tbls.rdd.collect())
  tbl_details = []
  # Iterate through each table and get details
  if len(tbl_list)>0:
      for tbl in tbl_list:
        try:
          # Describe each table to get details
          tblDetails = spark.sql("describe detail "+tbl.database+"."+tbl.tableName)
          tblDetails=tblDetails.withColumn("dbname",lit(tbl.database))
          tbl_details.extend(tblDetails.rdd.collect())
        except Exception as e:
          pass
  # Return the list of tables with details
  return tbl_details

# COMMAND ----------

tables = get_tbls_dets(dbName)

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
if len(tables)>0:
  tbldetDF = spark.createDataFrame(tables,schema=tSchema)
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

# MAGIC %md
# MAGIC ##### 3. Get DBFS File Statistics

# COMMAND ----------

# Function to walk through the directory structure to gather file stats
def analyzeDir(parentDir,dbfsPath):
  fileList = []
  badDirList = []
  try:
    sourceFiles = dbutils.fs.ls(parentDir)
  except:
    badDirList.append(subDir)

  #Get list of files which have some data in them
  while len(sourceFiles)>0:
    fileDetails = sourceFiles.pop()
    if fileDetails.size>0:
      fileDetailsList = list(fileDetails)
      fileDetailsList.append(dbfsPath)
      fileList.append(fileDetailsList)
    else:
      try:
        subDirFiles = dbutils.fs.ls(fileDetails.path)
        sourceFiles.extend(subDirFiles)
      except:
        badDirList.append(fileDetails.path) 
  return fileList

# COMMAND ----------

# Get all the locations of tables
tableLocations = tbldetDF.select("location").collect()
tableFiles = []

# Walk through each table location and gather file stats
while len(tableLocations)>0:
  dbfsPath = tableLocations.pop()[0]
  tablePath = dbfsPath.replace("dbfs:","")+"/"
  fileList = analyzeDir(tablePath,dbfsPath)
  tableFiles.extend(fileList)


# COMMAND ----------

# Create a temporary table out of the data collected
tSchema = StructType([StructField("path", StringType())\
                      ,StructField("name", StringType())\
                      ,StructField("size", LongType())\
                      ,StructField("modificationTime", LongType())\
                      ,StructField("tablePath", StringType())
                     ])
if len(tableFiles)>0:
  filesDF = spark.createDataFrame(tableFiles,schema=tSchema)
  filesDF.createOrReplaceTempView("files_tmp")
else:
  print("No files found")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace temporary view file_stats
# MAGIC as
# MAGIC select tablePath,path,size,
# MAGIC split(replace(path,tablePath,''),'/')[1] as subPath,
# MAGIC case when substr(split(replace(path,tablePath,''),'/')[1],-7) = 'parquet' or split(replace(path,tablePath,''),'/')[1] = '_delta_log' then 0 else 1 end as partitionDir,
# MAGIC case when split(replace(path,tablePath,''),'/')[1] = '_delta_log' then 1 else 0 end as deltaLogDir
# MAGIC from files_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Persist the table metadata to a database called 'dataops'

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists dataops

# COMMAND ----------

#%sql
#drop table if exists dataops.tbl_details

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dataops.tbl_details as select * from table_stats

# COMMAND ----------

#%sql
#drop table if exists dataops.tbl_file_details

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists dataops.tbl_file_details as select * from file_stats

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

# MAGIC %sql
# MAGIC -- If table already exists
# MAGIC merge into dataops.tbl_file_details as t
# MAGIC using 
# MAGIC file_stats as s
# MAGIC on t.path = s.path 
# MAGIC when matched then update set *
# MAGIC when not matched then insert *;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Summarize Statistics

# COMMAND ----------

# DBTITLE 1,Top 5 Tables
# MAGIC %sql
# MAGIC -- Top 5 tables
# MAGIC select 
# MAGIC dbname,table_name,sizeInGB,partitionFlag
# MAGIC from dataops.tbl_details 
# MAGIC order by sizeInGB desc
# MAGIC limit 5
