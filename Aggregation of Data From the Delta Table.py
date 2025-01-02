# Databricks notebook source
# MAGIC %md
# MAGIC In the Transaction Data Preparation, we have enabled "delta.enableChangeDataFeed" = true, this means it will give us the info about the record if any update has been done or not. Lets say If the payment was done and after few days the payment status is Refunded, here in this case we have to know when it was updated and what is the previous status
# MAGIC
# MAGIC
# MAGIC Create a Table for aggregated Data, so that we can calculate the sales, profit and refunded amount 

# COMMAND ----------

aggregated_table = "transaction_catalog.default.upi_agg_table"
raw_table = "transaction_catalog.default.upi_transaction_table"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {aggregated_table}
(
    merchant_id STRING,
    total_sales DOUBLE,
    total_refunds DOUBLE,
    net_sales DOUBLE
)
USING DELTA     
""")

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

def process_aggregation(batch_df,batch_id):
    print(f"Processing batch: {batch_id}")

    aggregated_df = batch_df.groupBy("merchant_id").agg(
        sum( when(col("transaction_status") == "completed", col("transaction_amount")).otherwise(0) ).alias("total_sales"),
        sum( when(col("transaction_status") == "refunded", -col("transaction_amount")).otherwise(0) ).alias("total_refunds")
    ).withColumn("net_sales", col("total_sales") + col("total_refunds"))

    deltaTable = DeltaTable.forName(spark, aggregated_table)
    deltaTable.alias("target").merge(
        aggregated_df.alias("source"),"target.merchant_id = source.merchant_id"
    ).whenMatchedUpdate(set={
        "total_sales": "source.total_sales + target.total_sales",
        "total_refunds": "source.total_refunds + target.total_refunds",
        "net_sales": "source.net_sales + target.net_sales"
    }).whenNotMatchedInsertAll().execute()


cdc_stream = spark.readStream.format("delta").option("readChangeFeed","true").table(raw_table)
print("Read Stream Started.........")

cdc_stream.writeStream.foreachBatch(process_aggregation).outputMode("update").start().awaitTermination()
print("Write Stream Started.........")
# query = data.writeStream \
#     .outputMode("append") \
#     .format("parquet") \
#     .option("path", "hdfs://path/to/hive/table/location") \
#     .option("checkpointLocation", checkpoint_dir) \
#     .start()

