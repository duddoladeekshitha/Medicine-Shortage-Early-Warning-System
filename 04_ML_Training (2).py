# Databricks notebook source

#LOAD GOLD TABLE
df_gold = spark.table("pharma_catalog.raw.gold_features")

display(df_gold)



#  FILL NULLS
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


# LABEL INDEXING
from pyspark.ml.feature import StringIndexer

indexer = StringIndexer(inputCol="label", outputCol="label_index")
df_ml = indexer.fit(df_gold).transform(df_gold)


# FEATURE ASSEMBLER
from pyspark.ml.feature import VectorAssembler

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

df_ml_ready = assembler.transform(df_ml)


#TRAIN / TEST SPLIT
train_df, test_df = df_ml_ready.randomSplit([0.8, 0.2], seed=42)



#TRAIN RANDOM FOREST MODEL
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label_index",
    numTrees=50
)

model = rf.fit(train_df)
predictions = model.transform(test_df)



#EVALUATE MODEL
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index",
    predictionCol="prediction",
    metricName="f1"
)

f1 = evaluator.evaluate(predictions)
print("F1 Score:", f1)



#SAVE MODEL 
import mlflow
import mlflow.spark
import os

# Model save path in your Volume
model_path = "/Volumes/pharma_catalog/raw/pharma_volume/models/rf_model"

# Remove old model if exists
dbutils.fs.rm(model_path, recurse=True)

# MLflow temporary directory fix
os.environ["MLFLOW_DFS_TMP"] = "/Volumes/pharma_catalog/raw/pharma_volume/models/"

with mlflow.start_run():
    mlflow.log_metric("f1_score", f1)
    mlflow.spark.save_model(model, model_path)

print("MODEL SAVED SUCCESSFULLY AT:", model_path)