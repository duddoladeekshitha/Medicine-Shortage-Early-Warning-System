# Databricks notebook source
# Load Bronze Tables
df_pharma = spark.table("pharma_catalog.raw.bronze_pharma")
df_drugs = spark.table("pharma_catalog.raw.bronze_drugs")
df_supplier = spark.table("pharma_catalog.raw.bronze_supplier")
df_prescriptions = spark.table("pharma_catalog.raw.bronze_prescriptions")

from pyspark.sql.functions import lower, trim, col

# Clean drug names
df_pharma = df_pharma.withColumn("drug_name_clean", lower(trim(col("drug_name"))))
df_drugs = df_drugs.withColumn("drug_name_clean", lower(trim(col("genericName"))))

# Join pharma + drugs
df_pharma_drugs = df_pharma.join(
    df_drugs,
    on="drug_name_clean",
    how="left"
)

# Join supplier
df_pharma_drugs_supplier = df_pharma_drugs.join(
    df_supplier,
    on="supID",
    how="left"
)

# Join prescriptions
df_silver_master = df_pharma_drugs_supplier.join(
    df_prescriptions,
    on="NDC",
    how="left"
)

display(df_silver_master)

# COMMAND ----------

df_silver_master.write.format("delta").mode("overwrite").saveAsTable(
    "pharma_catalog.raw.silver_master"
)