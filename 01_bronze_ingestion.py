# Databricks notebook source
vol = "/Volumes/pharma_catalog/raw/pharma_volume/"

df_pharma = spark.read.csv(vol + "data.csv", header=True, inferSchema=True)
df_drugs = spark.read.csv(vol + "DRUGS.csv", header=True, inferSchema=True)
df_supplier = spark.read.csv(vol + "SUPPLIER.csv", header=True, inferSchema=True)
df_prescriptions = spark.read.csv(vol + "PRESCRIPTIONS.csv", header=True, inferSchema=True)

display(df_pharma)


# COMMAND ----------

df_pharma.write.format("delta").mode("overwrite").saveAsTable("pharma_catalog.raw.bronze_pharma")
df_drugs.write.format("delta").mode("overwrite").saveAsTable("pharma_catalog.raw.bronze_drugs")
df_supplier.write.format("delta").mode("overwrite").saveAsTable("pharma_catalog.raw.bronze_supplier")
df_prescriptions.write.format("delta").mode("overwrite").saveAsTable("pharma_catalog.raw.bronze_prescriptions")


# COMMAND ----------

spark.sql("SHOW TABLES IN pharma_catalog.raw")