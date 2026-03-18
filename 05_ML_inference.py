# Databricks notebook source
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="label", outputCol="label_index")
df_inf = indexer.fit(df_inf).transform(df_inf)

# COMMAND ----------

# LOAD GOLD FEATURES
df_gold = spark.table("pharma_catalog.raw.gold_features")

# Fill nulls (same as training)
df_gold = df_gold.fillna({
    "quantity_in_stock": 0,
    "stock_coverage_days": 0,
    "is_low_stock": 0,
    "supplier_reliability_score": 0.5,
    "profit_margin": 0,
    "demand_per_day": 0,
    "risk_score": 0,
    "purchasePrice": 0,
    "sellPrice": 0,
    "qty": 1,
    "days": 1
})

#VECTOR ASSEMBLY
from pyspark.ml.feature import VectorAssembler, StringIndexer

feature_cols = [
    "quantity_in_stock",
    "stock_coverage_days",
    "is_low_stock",
    "supplier_reliability_score",
    "profit_margin",
    "demand_per_day",
    "risk_score",
    "purchasePrice",
    "sellPrice"
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="keep"
)

df_inf = assembler.transform(df_gold)

# fixes label_index error
indexer = StringIndexer(inputCol="label", outputCol="label_index")
df_inf = indexer.fit(df_inf).transform(df_inf)

#RE-TRAIN SMALL MODEL (valid for inference)
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label_index",
    numTrees=30
)

model = rf.fit(df_inf)

#GENERATE PREDICTIONS
preds = model.transform(df_inf)

display(preds.select(
    "drug_name",
    "facility_name",
    "quantity_in_stock",
    "prediction",
    "probability",
    "risk_score"
))

# SAVE AS FINAL PREDICTION TABLE
spark.sql("DROP TABLE IF EXISTS pharma_catalog.raw.predicted_shortages")

preds.select(
    "drug_name",
    "facility_name",
    "quantity_in_stock",
    "prediction",
    "probability",
    "risk_score",
    "stock_coverage_days",
    "supplier_reliability_score"
).write.format("delta").mode("overwrite").saveAsTable(
    "pharma_catalog.raw.predicted_shortages"
)

print("Inference Completed. Saved Table: pharma_catalog.raw.predicted_shortages")