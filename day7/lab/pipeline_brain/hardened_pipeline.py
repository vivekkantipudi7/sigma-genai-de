import shutil
import logging
import json
import os
from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, broadcast, when, sum, count, max, avg, min, coalesce, mode
from pyspark.sql.types import StringType, FloatType, DateType

logging.basicConfig(level=logging.INFO)

def ingest_bronze(spark, input_path, output_path, run_date, run_id):
    try:
        logging.info("Starting ingest_bronze stage")
        transactions_df = (spark.read.option("header", "true")
                          .option("inferSchema", "false")
                          .csv(input_path)
                          .withColumn("ingestion_timestamp", lit(run_date))
                          .withColumn("source_file", lit("transactions.csv"))
                          .withColumn("pipeline_run_id", lit(run_id)))
        
        merchants_df = (spark.read.option("header", "true")
                        .option("inferSchema", "false")
                        .csv(input_path.replace("transactions", "merchants"))
                        .withColumn("ingestion_timestamp", lit(run_date))
                       .withColumn("source_file", lit("merchants.csv"))
                       .withColumn("pipeline_run_id", lit(run_id)))
        
        partition_path_transactions = os.path.join(output_path, "transactions", f"ingestion_timestamp={run_date}")
        partition_path_merchants = os.path.join(output_path, "merchants", f"ingestion_timestamp={run_date}")
        
        shutil.rmtree(partition_path_transactions, ignore_errors=True)
        shutil.rmtree(partition_path_merchants, ignore_errors=True)
        
        transactions_df.write.partitionBy("ingestion_timestamp").parquet(os.path.join(output_path, "transactions"))
        merchants_df.write.partitionBy("ingestion_timestamp").parquet(os.path.join(output_path, "merchants"))
        
        logging.info(f"[Stage: ingest_bronze] ingested {transactions_df.count():,} transactions and {merchants_df.count():,} merchants")
    except Exception as e:
        logging.error(f"Error in ingest_bronze stage: {e}")
        raise

def transform_silver(spark, bronze_path, merchants_path, output_path, run_date):
    try:
        logging.info("Starting transform_silver stage")
        transactions_df = (spark.read.parquet(bronze_path)
                          .where(col("ingestion_timestamp") == run_date))
        
        merchants_df = (spark.read.parquet(merchants_path)
                        .where(col("ingestion_timestamp") == run_date)
                       .cache())  # Cache the small merchants table
        
        transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
        transactions_df = transactions_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
        transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType()))
        transactions_df = transactions_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))
        
        transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
        logging.info(f"[Stage: transform_silver] after filtering: {transactions_df.count():,} rows")
        
        window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
        transactions_df = (transactions_df.withColumn("rank", col("transaction_id").rank().over(window))
                          .filter(col("rank") == 1)
                         .drop("rank"))
        logging.info(f"[Stage: transform_silver] after deduplication: {transactions_df.count():,} rows")
        
        transactions_df = (transactions_df.join(broadcast(merchants_df), "merchant_id", "left")
                           .withColumn("quality_flag", when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN")))
        
        partition_path = os.path.join(output_path, f"transaction_date={run_date}")
        shutil.rmtree(partition_path, ignore_errors=True)
        
        transactions_df.write.partitionBy("transaction_date").parquet(output_path)
        logging.info(f"[Stage: transform_silver] output: {transactions_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in transform_silver stage: {e}")
        raise

def build_merchant_performance(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_merchant_performance stage")
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)  # Partition pruning
        
        merchant_performance_df = silver_df.groupBy("merchant_id", "merchant_name", "category", "city", "transaction_date") \
          .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("txn_count"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        partition_path = os.path.join(output_path, "merchant_performance", f"transaction_date={run_date}")
        shutil.rmtree(partition_path, ignore_errors=True)
        
        merchant_performance_df.write.mode("overwrite").partitionBy("transaction_date").parquet(os.path.join(output_path, "merchant_performance"))
        logging.info(f"[Stage: build_merchant_performance] output: {merchant_performance_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in build_merchant_performance stage: {e}")
        raise

def build_customer_ltv(spark, silver_path, output_path):
    try:
        logging.info("Starting build_customer_ltv stage")
        silver_df = spark.read.parquet(silver_path)
        
        customer_ltv_df = silver_df.filter(col("status") == "COMPLETED") \
            .groupBy("customer_id") \
            .agg(
                sum("amount").alias("total_spent"),
                count("*").alias("total_txns"),
                avg("amount").alias("avg_txn_value"),
                min("transaction_date").alias("first_txn_date"),
                max("transaction_date").alias("last_txn_date"),
                coalesce(mode("payment_method").over(Window.partitionBy("customer_id")), lit("N/A")).alias("preferred_payment_method")
            )
        
        shutil.rmtree(os.path.join(output_path, "customer_ltv"), ignore_errors=True)
        
        customer_ltv_df.write.mode("overwrite").parquet(os.path.join(output_path, "customer_ltv"))
        logging.info(f"[Stage: build_customer_ltv] output: {customer_ltv_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in build_customer_ltv stage: {e}")
        raise

def build_daily_summary(spark, silver_path, output_path, run_date):
    try:
        logging.info("Starting build_daily_summary stage")
        silver_df = spark.read.parquet(silver_path).filter(col("transaction_date") == run_date)  # Partition pruning
        
        daily_summary_df = silver_df.groupBy("transaction_date") \
           .agg(
                sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
                count("*").alias("total_txns"),
                count(distinct("customer_id")).alias("unique_customers"),
                count(distinct("merchant_id")).alias("unique_merchants"),
                (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct")
            )
        
        partition_path = os.path.join(output_path, "daily_summary", f"transaction_date={run_date}")
        shutil.rmtree(partition_path, ignore_errors=True)
        
        daily_summary_df.write.mode("overwrite").partitionBy("transaction_date").parquet(os.path.join(output_path, "daily_summary"))
        logging.info(f"[Stage: build_daily_summary] output: {daily_summary_df.count():,} rows")
    except Exception as e:
        logging.error(f"Error in build_daily_summary stage: {e}")
        raise

def run_gold(spark, silver_path, gold_output_dir, run_date):
    try:
        logging.info("Starting run_gold stage")
        build_merchant_performance(spark, silver_path, gold_output_dir, run_date)
        build_customer_ltv(spark, silver_path, gold_output_dir)
        build_daily_summary(spark, silver_path, gold_output_dir, run_date)
        
        run_metadata = {
            "run_date": run_date,
            "silver_path": silver_path,
            "gold_output_dir": gold_output_dir,
            "status": "success",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        spark.sparkContext.parallelize([run_metadata]).write.json(os.path.join(gold_output_dir, "run_metadata"))
    except Exception as e:
        logging.error(f"Error in run_gold stage: {e}")
        raise

def main():
    spark = (SparkSession.builder
            .appName("Sigma DataTech Transaction Analytics Pipeline")
             .getOrCreate())
    
    input_path = "s3://sigma-datatech/raw/transactions/"
    merchants_path = "s3://sigma-datatech/raw/merchants/"
    bronze_output_path = "s3://sigma-datatech/bronze/"
    silver_output_path = "s3://sigma-datatech/silver/"
    gold_output_dir = "s3://sigma-datatech/gold/"
    
    run_date = "2026-05-27"
    run_id = "run_id_12345"
    
    try:
        ingest_bronze(spark, input_path, bronze_output_path, run_date, run_id)
        transform_silver(spark, os.path.join(bronze_output_path, "transactions"), merchants_path, silver_output_path, run_date)
        run_gold(spark, silver_output_path, gold_output_dir, run_date)
        
        run_metadata = {
            "run_date": run_date,
            "run_id": run_id,
            "status": "SUCCESS",
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        with open("s3://sigma-datatech/metadata/run_metadata_2026-05-27.json", "w") as f:
            json.dump(run_metadata, f)
    except Exception as e:
        run_metadata = {
            "run_date": run_date,
            "run_id": run_id,
            "status": "FAILED",
            "error_message": str(e),
            "started_at": datetime.now().isoformat(),
            "completed_at": datetime.now().isoformat()
        }
        with open("s3://sigma-datatech/metadata/run_metadata_2026-05-27.json", "w") as f:
            json.dump(run_metadata, f)
        raise

if __name__ == "__main__":
    main()
