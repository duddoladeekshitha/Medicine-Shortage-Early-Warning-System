# Databricks notebook source
#load the silver table
df_silver = spark.table("pharma_catalog.raw.silver_master")
display(df_silver)

# COMMAND ----------

#Rename name to supplier name
df_gold = df_silver.withColumnRenamed("name", "supplier_name")
df_gold = df_gold.withColumnRenamed("address", "supplier_address")
df_gold = df_gold.withColumnRenamed("phone", "supplier_phone")

# COMMAND ----------

#Create stock_coverage_days
from pyspark.sql.functions import *

df_gold = df_gold.withColumn(
    "stock_coverage_days",
    col("quantity_in_stock") / col("qty")
)

# COMMAND ----------

#create is_low_stock
df_gold = df_gold.withColumn(
    "is_low_stock",
    (col("stock_coverage_days") < 7).cast("int")
)

# COMMAND ----------

#Create supplier_reliability_score
df_gold = df_gold.withColumn(
    "supplier_reliability_score",
    when(col("supplier_name").isNull(), 0.5).otherwise(1.0)
)

# COMMAND ----------

#create profit margin
df_gold = df_gold.withColumn(
    "profit_margin",
    col("sellPrice") - col("purchasePrice")
)

# COMMAND ----------

#create demand_per_day
df_gold = df_gold.withColumn(
    "demand_per_day",
    when(col("days").isNull(), None).otherwise(col("qty") / col("days"))
)


# COMMAND ----------

#create risk score
df_gold = df_gold.withColumn(
    "risk_score",
    ( (1/(col("stock_coverage_days")+1)) * 0.4 ) +
    ( col("demand_per_day") * 0.3 ) +
    ( (1 - col("supplier_reliability_score")) * 0.3 )
)

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("pharma_catalog.raw.gold_features")

# COMMAND ----------

spark.sql("SELECT * FROM pharma_catalog.raw.gold_features LIMIT 10").show()

# COMMAND ----------

df_gold.columns

# COMMAND ----------

df_gold.select("profit_margin").show(5)


# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS pharma_catalog.raw.gold_features")