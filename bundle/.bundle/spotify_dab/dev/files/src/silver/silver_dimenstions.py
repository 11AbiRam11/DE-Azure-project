# Databricks notebook source
# MAGIC %md
# MAGIC ### **DimUser**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.transformations import reusable

# COMMAND ----------

df = spark.read.format("parquet")\
  .load("abfss://bronze@0storageaccount.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### changing the current working directory to aceess the uitls folder

# COMMAND ----------

import os
import sys
project_pth = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_pth)
os.getcwd()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Autoloader**

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimUser/checkpoint")\
    .load("abfss://bronze@0storageaccount.dfs.core.windows.net/DimUser")
display(df_user)


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Transformation**

# COMMAND ----------

# changing the user_name lower case to upper case
df_user = df_user.withColumn("user_name", upper(col("user_name")))
display(df_user)

# COMMAND ----------

from utils.transformations import reusable
df_user_obj = reusable()

df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
display(df_user)

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@0storageaccount.dfs.core.windows.net/DimUser/data")\
    .toTable("spotify_catalog.silver.DimUser") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimArtist**

# COMMAND ----------

df_art = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimArtist/checkpoint")\
    .load("abfss://bronze@0storageaccount.dfs.core.windows.net/DimArtist")
display(df_art)

# COMMAND ----------

df_art = df_art.withColumn("artist_name", upper(col("artist_name")))
display(df_art)

# COMMAND ----------

df_art_obj = reusable()

df_art = df_art_obj.dropColumns(df_art, ['_rescued_data'])
df_art = df_art.dropDuplicates(['artist_id'])
display(df_art)

# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimArtist/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@0storageaccount.dfs.core.windows.net/DimArtist/data")\
    .toTable("spotify_catalog.silver.DimArtist") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dimtrack**

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimTrack/checkpoint")\
    .load("abfss://bronze@0storageaccount.dfs.core.windows.net/DimTrack")
display(df_track)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tranformation

# COMMAND ----------

df_track = df_track.withColumn("Duration_flag", when(col('duration_sec') < 150 , "short")\
                                                .when(col('duration_sec') < 300 , "medium")\
                                                .otherwise("long"))
df_track = df_track.withColumn("track_name", regexp_replace(col("track_name"), "-", " "))
display(df_track)

# COMMAND ----------

df_track_obj = reusable()

df_track = df_track.withColumn("track_name", upper(col("track_name")))
df_track = df_track_obj.dropColumns(df_track, ['_rescued_data'])
display(df_track)

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@0storageaccount.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_catalog.silver.DimTrack") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimDate**

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimDate/checkpoint")\
    .load("abfss://bronze@0storageaccount.dfs.core.windows.net/DimDate")
display(df_date)

# COMMAND ----------

df_date_obj = reusable()

df_date = df_date_obj.dropColumns(df_date, ['_rescued_data'])
display(df_date)

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@0storageaccount.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_catalog.silver.DimDate") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FactStream**

# COMMAND ----------

df_fact = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet")\
    .option("cloudFiles.schemaLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/FactStream/checkpoint")\
    .load("abfss://bronze@0storageaccount.dfs.core.windows.net/FactStream")
display(df_fact)

# COMMAND ----------


df_fact_obj = reusable()

df_fact = df_fact_obj.dropColumns(df_fact, ['_rescued_data'])

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@0storageaccount.dfs.core.windows.net/DimFactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@0storageaccount.dfs.core.windows.net/DimFactStream/data")\
    .toTable("spotify_catalog.silver.FactStream") 

# COMMAND ----------

