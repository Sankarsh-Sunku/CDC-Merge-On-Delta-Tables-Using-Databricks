# Databricks notebook source
# MAGIC %md
# MAGIC We need to create a delta table, before that we have to create a catalog and inside that we have to create that table
# MAGIC

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS transaction_catalog.default.upi_transaction_table 
(
    transaction_id STRING,
    upi_id STRING,
    merchant_id STRING,
    transaction_amount DOUBLE,
    transaction_timestamp TIMESTAMP,
    transaction_status STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = true)
          """)

print("Delta Table has been Created for storing the mocked data")

# COMMAND ----------

# MAGIC %md
# MAGIC Preparation of the mocked data using the DataFrame API

# COMMAND ----------

from delta.tables import *
import time

mocked_batches = [
    spark.createDataFrame([
        ("T001", "upi1@bank", "M001", 500.0, "2024-12-21 10:00:00", "initiated"),
        ("T002", "upi2@bank", "M002", 1000.0, "2024-12-21 10:05:00", "initiated"),
        ("T003", "upi3@bank", "M003", 1500.0, "2024-12-21 10:10:00", "initiated"),
    ], ["transaction_id", "upi_id", "merchant_id", "transaction_amount", "transaction_timestamp", "transaction_status"]),
    spark.createDataFrame([
        ("T001", "upi1@bank", "M001", 500.0, "2024-12-21 10:15:00", "completed"),  # Update transaction
        ("T002", "upi2@bank", "M002", 1000.0, "2024-12-21 10:20:00", "failed"),    # Update transaction
        ("T004", "upi4@bank", "M004", 2000.0, "2024-12-21 10:25:00", "initiated"), # New transaction
    ], ["transaction_id", "upi_id", "merchant_id", "transaction_amount", "transaction_timestamp", "transaction_status"]),
    spark.createDataFrame([
        ("T001", "upi1@bank", "M001", 500.0, "2024-12-21 10:30:00", "refunded"),  # Refund issued
        ("T003", "upi3@bank", "M003", 1500.0, "2024-12-21 10:35:00", "completed"), # Completed transaction
    ], ["transaction_id", "upi_id", "merchant_id", "transaction_amount", "transaction_timestamp", "transaction_status"])
]

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert Data to a table using Delta Lake

# COMMAND ----------

from spark.sql.functions import *


def mergeRecordsToTheDeltaTable(batch,table_name):
    deltaTable = DeltaTabele.forName(spark,table_name)

    deltaTable.alias("target").merge(batch.alias("source"), "target.transaction_id = source.transaction_id")\
        .whenMatchedUpdate(set={
            "upi_id": "source.upi_id",
            "merchant_id": "source.merchant_id",
            "transaction_amount": "source.transaction_amount",
            "transaction_timestamp": "source.transaction_timestamp",
            "transaction_status": "source.transaction_status"
        })\
        .whenNotMatchedInsertAll().execute()

# COMMAND ----------


for i in range(len(mocked_batches)):
    mergeRecordsToTheDeltaTable("transaction_catalog.default.upi_transaction_table", mocked_batches[i])
    print(f"Batch processed successfully.")
    time.sleep(4)
