# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### Delta Lake Table Profiler v0.1
# MAGIC 
# MAGIC #### Scope
# MAGIC This notebook analyzes a Delta lake table stored in a database.
# MAGIC It scans the table and dbfs file system to gather important statistics to help you find ways to improve the performance of the Delta lake table
# MAGIC 
# MAGIC Here are the steps we execute
# MAGIC * Read database name and table name entered by user at the top of the notebook
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
# MAGIC - Opportunities for Vacuuming old files
# MAGIC 
# MAGIC #### Instructions
# MAGIC 
# MAGIC 1.  Enter the database name in the text box on the top
# MAGIC 2.  Click "Run All" in the menu bar at the top of your browser window

# COMMAND ----------

# Setup Notebook Parameters
# dbutils.widgets.removeAll()
# dbutils.widgets.text("dbname", "abe_demo","DB Name")
# dbutils.widgets.text("table_name", "batch_bronze","Table Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Get Database Name

# COMMAND ----------

# Get the database name from the notebook widget at the top
dbName = getArgument("dbname")
tableName = getArgument("table_name")
print("This profiler will analyze table:{} in the database:{}".format(tableName,dbName))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Get Table Statistics

# COMMAND ----------

import json, requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

tbl_details = []
try:
  tblDetails = spark.sql("describe detail "+dbName+"."+tableName)
  tblDetails=tblDetails.withColumn("dbname",lit(dbName))
  tbl_details.extend(tblDetails.rdd.collect())
except Exception as e:
   pass

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
display(tbldetDF)

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
    # Ensure it's not a directory by checking size and the last character
    if fileDetails.size>0 or fileDetails.name[-1:]!='/':
      fileDetailsList = list(fileDetails)
      fileDetailsList.append(dbfsPath)
      fileList.append(fileDetailsList)
    else:
      try:
        print("processing subdir :"+fileDetails.path)
        subDirFiles = dbutils.fs.ls(fileDetails.path)
        print("File count found :"+str(len(subDirFiles)))
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
                      #,StructField("modificationTime", LongType())\
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

#%sql
#create table if not exists dataops.tbl_file_details as select * from file_stats

# COMMAND ----------

# %sql
# -- If table already exists
# merge into dataops.tbl_file_details as t
# using 
# file_stats as s
# on t.path = s.path 
# when matched then update set *
# when not matched then insert *;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Summarize Statistics

# COMMAND ----------

# DBTITLE 1,Row Count
display(spark.sql("select round(count(*)/1000000,2) as RowCountInMM from "+dbName+"."+tableName))

# COMMAND ----------

# DBTITLE 1,Table Details
# MAGIC %sql
# MAGIC select * from table_stats

# COMMAND ----------

# DBTITLE 1,File Level Details
# MAGIC %sql
# MAGIC select * from file_stats order by size desc

# COMMAND ----------

# DBTITLE 1,Actual Partition Count
# MAGIC %sql
# MAGIC select count(distinct subPath) as partitions_count
# MAGIC from file_stats
# MAGIC where partitionDir = 1 and deltaLogDir <> 1

# COMMAND ----------

# DBTITLE 1,Table Partition Skew
# MAGIC %sql
# MAGIC 
# MAGIC select tablePath,subPath,round(sum(size)/1024/1024,2) as partitionInMB,count(*) filesPerPartition,round(sum(size)/count(*)/1024/1024,3) avgFileSizeInMB 
# MAGIC from file_stats
# MAGIC where partitionDir = 1 and deltaLogDir <> 1
# MAGIC group by tablePath,subPath
# MAGIC order by 3 desc

# COMMAND ----------

# DBTITLE 1,Opportunities for Vacuum
# MAGIC %sql
# MAGIC -- This query compares the file count between current version and the total file count for all versions
# MAGIC select 
# MAGIC t.dbname,t.table_name,t.format,
# MAGIC t.location,t.sizeInGB tableSize,t.numFiles tableFileCount,
# MAGIC f.sizeInGB as totalFileSize,f.numFiles as totalFileCount
# MAGIC from 
# MAGIC table_stats t,
# MAGIC (select tablePath,round(sum(size)/1024/1024/1024,3) sizeInGB,count(*) numFiles from file_stats group by tablePath) f
# MAGIC where
# MAGIC t.location = f.tablePath

# COMMAND ----------

#%sql
#select tablePath,count(*) from dataops.tbl_file_details group by tablePath
