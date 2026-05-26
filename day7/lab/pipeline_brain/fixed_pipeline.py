"""
Sigma DataTech Transaction Analytics Pipeline
Fixed version after Module 5 code review.
This file includes explicit error handling, parameterized paths, row count logging,
and basic schema validation for the Bronze -> Silver -> Gold pipeline.
"""

import argparse
import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    count,
    countDistinct,
    lit,
    max as spark_max,
    min as spark_min,
    row_number,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import DateType, FloatType, StringType
from pyspark.sql.window import Window

DEFAULT_INPUT_PATH = os.getenv("INPUT_PATH", "s3://sigma-datatech/raw/transactions/")
DEFAULT_MERCHANTS_PATH = os.getenv("MERCHANTS_PATH", "s3://sigma-datatech/raw/merchants/")
DEFAULT_BRONZE_PATH = os.getenv("BRONZE_OUTPUT_PATH", "s3://sigma-datatech/bronze/")
DEFAULT_SILVER_PATH = os.getenv("SILVER_OUTPUT_PATH", "s3://sigma-datatech/silver/")
DEFAULT_GOLD_PATH = os.getenv("GOLD_OUTPUT_PATH", "s3://sigma-datatech/gold/")
DEFAULT_METADATA_PATH = os.getenv("METADATA_OUTPUT_PATH", "s3://sigma-datatech/metadata/")
DEFAULT_RUN_DATE = os.getenv("RUN_DATE", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
DEFAULT_RUN_ID = os.getenv("RUN_ID", f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}")

EXPECTED_SILVER_COLUMNS = [
    "transaction_id",
    "amount",
    "status",
    "merchant_id",
    "customer_id",
    "transaction_date",
    "payment_method",
    "merchant_name",
    "category",
    "city",
    "quality_flag",
    "ingestion_timestamp",
    "pipeline_run_id",
]


def log_row_count(stage_name: str, df: DataFrame) -> None:
    count_value = df.count()
    print(f"[ROW_COUNT] {stage_name}: {count_value:,} rows")


def validate_schema(df: DataFrame, expected_columns: List[str], dataset_name: str) -> None:
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(
            f"Schema validation failed for {dataset_name}. Missing columns: {sorted(missing)}"
        )
    print(f"[SCHEMA] {dataset_name} contains expected columns")


def ingest_bronze(
    spark: SparkSession,
    transactions_input_path: str,
    merchants_input_path: str,
    bronze_output_dir: str,
    run_date: str,
    run_id: str,
) -> None:
    print(f"[START] Ingest Bronze: transactions={transactions_input_path}, merchants={merchants_input_path}")

    transactions_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(transactions_input_path)
        .withColumn("ingestion_timestamp", lit(run_date))
        .withColumn("source_file", lit(os.path.basename(transactions_input_path.rstrip("/"))))
        .withColumn("pipeline_run_id", lit(run_id))
    )
    log_row_count("Bronze transactions read", transactions_df)

    merchants_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(merchants_input_path)
        .withColumn("ingestion_timestamp", lit(run_date))
        .withColumn("source_file", lit(os.path.basename(merchants_input_path.rstrip("/"))))
        .withColumn("pipeline_run_id", lit(run_id))
    )
    log_row_count("Bronze merchants read", merchants_df)

    transactions_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(
        os.path.join(bronze_output_dir, "transactions")
    )
    merchants_df.write.mode("overwrite").partitionBy("ingestion_timestamp").parquet(
        os.path.join(bronze_output_dir, "merchants")
    )
    print("[COMPLETE] Bronze ingestion finished")


def transform_silver(
    spark: SparkSession,
    bronze_transactions_path: str,
    merchants_path: str,
    silver_output_dir: str,
    run_date: str,
) -> None:
    print(f"[START] Transform Silver: bronze={bronze_transactions_path}, merchants={merchants_path}")

    transactions_df = spark.read.parquet(bronze_transactions_path).where(col("ingestion_timestamp") == run_date)
    merchants_df = spark.read.parquet(merchants_path).where(col("ingestion_timestamp") == run_date).cache()

    validate_schema(transactions_df, [
        "transaction_id",
        "amount",
        "merchant_id",
        "transaction_date",
        "status",
    ],
        "bronze transactions",
    )

    transactions_df = transactions_df.withColumn("amount", col("amount").cast(FloatType()))
    transactions_df = transactions_df.withColumn("transaction_date", col("transaction_date").cast(DateType()))
    transactions_df = transactions_df.withColumn("transaction_id", col("transaction_id").cast(StringType()))
    transactions_df = transactions_df.withColumn("merchant_id", col("merchant_id").cast(StringType()))

    log_row_count("Silver after cast", transactions_df)

    transactions_df = transactions_df.filter((col("transaction_id").isNotNull()) & (col("amount") >= 0))
    log_row_count("Silver after filtering invalid rows", transactions_df)

    dedup_window = Window.partitionBy("transaction_id").orderBy(col("ingestion_timestamp").desc())
    transactions_df = (
        transactions_df.withColumn("row_number", row_number().over(dedup_window))
        .filter(col("row_number") == 1)
        .drop("row_number")
    )
    log_row_count("Silver after deduplication", transactions_df)

    transactions_df = (
        transactions_df.join(broadcast(merchants_df), "merchant_id", "left")
        .withColumn(
            "quality_flag",
            when(col("merchant_id").isNull(), "UNMATCHED").otherwise("CLEAN"),
        )
    )
    log_row_count("Silver after enrichment", transactions_df)

    transactions_df.write.mode("overwrite").partitionBy("transaction_date").parquet(silver_output_dir)
    print("[COMPLETE] Silver transformation finished")


def build_merchant_performance(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    run_date: str,
) -> None:
    silver_df = spark.read.parquet(silver_path).filter(col("date") == run_date)
    merchant_performance_df = silver_df.groupBy(
        "merchant_id",
        "merchant_name",
        "category",
        "city",
        "date",
    ).agg(
        spark_sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
        count("*").alias("txn_count"),
        (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct"),
    )

    merchant_performance_df.write.mode("overwrite").partitionBy("date").parquet(
        f"{output_path.rstrip('/')}/merchant_performance"
    )


def build_customer_ltv(spark: SparkSession, silver_path: str, output_path: str) -> None:
    silver_df = spark.read.parquet(silver_path)
    completed_df = silver_df.filter(col("status") == "COMPLETED")

    customer_ltv_base = completed_df.groupBy("customer_id").agg(
        spark_sum("amount").alias("total_spent"),
        count("*").alias("total_txns"),
        avg("amount").alias("avg_txn_value"),
        spark_min("transaction_date").alias("first_txn_date"),
        spark_max("transaction_date").alias("last_txn_date"),
    )

    preferred_method = (
        completed_df.groupBy("customer_id", "payment_method")
        .agg(count("*").alias("method_count"))
        .withColumn("row_number", row_number().over(
            Window.partitionBy("customer_id").orderBy(col("method_count").desc(), col("payment_method"))
        ))
        .filter(col("row_number") == 1)
        .select("customer_id", col("payment_method").alias("preferred_payment_method"))
    )

    customer_ltv_df = customer_ltv_base.join(preferred_method, "customer_id", "left")
    customer_ltv_df.write.mode("overwrite").parquet(f"{output_path.rstrip('/')}/customer_ltv")


def build_daily_summary(
    spark: SparkSession,
    silver_path: str,
    output_path: str,
    run_date: str,
) -> None:
    silver_df = spark.read.parquet(silver_path).filter(col("date") == run_date)
    daily_summary_df = silver_df.groupBy("date").agg(
        spark_sum(when(col("status") == "COMPLETED", col("amount")).otherwise(0)).alias("total_revenue"),
        count("*").alias("total_txns"),
        countDistinct("customer_id").alias("unique_customers"),
        countDistinct("merchant_id").alias("unique_merchants"),
        (count(when(col("status") == "FAILED", 1)) / count("*") * 100).alias("failure_rate_pct"),
    )
    daily_summary_df.write.mode("overwrite").partitionBy("date").parquet(f"{output_path.rstrip('/')}/daily_summary")


def run_gold(spark: SparkSession, silver_path: str, gold_output_dir: str, run_date: str) -> None:
    print(f"[START] Build Gold layer: silver={silver_path}")
    build_merchant_performance(spark, silver_path, gold_output_dir, run_date)
    build_customer_ltv(spark, silver_path, gold_output_dir)
    build_daily_summary(spark, silver_path, gold_output_dir, run_date)
    print("[COMPLETE] Gold aggregation finished")


def safe_run(stage_name: str, fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as exc:
        print(f"[ERROR] {stage_name} failed: {type(exc).__name__}: {exc}")
        raise


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the fixed Sigma DataTech pipeline")
    parser.add_argument("--input-path", default=DEFAULT_INPUT_PATH)
    parser.add_argument("--merchants-path", default=DEFAULT_MERCHANTS_PATH)
    parser.add_argument("--bronze-path", default=DEFAULT_BRONZE_PATH)
    parser.add_argument("--silver-path", default=DEFAULT_SILVER_PATH)
    parser.add_argument("--gold-path", default=DEFAULT_GOLD_PATH)
    parser.add_argument("--metadata-path", default=DEFAULT_METADATA_PATH)
    parser.add_argument("--run-date", default=DEFAULT_RUN_DATE)
    parser.add_argument("--run-id", default=DEFAULT_RUN_ID)
    return parser.parse_args()


def main() -> None:
    args = get_args()

    spark = SparkSession.builder.appName("Sigma DataTech Transaction Analytics Pipeline").getOrCreate()

    bronze_transactions_path = args.input_path
    bronze_merchants_path = args.merchants_path
    bronze_output_base = args.bronze_path
    silver_output_base = args.silver_path
    gold_output_base = args.gold_path
    metadata_output_base = args.metadata_path

    safe_run(
        "Ingest Bronze",
        ingest_bronze,
        spark,
        bronze_transactions_path,
        bronze_merchants_path,
        bronze_output_base,
        args.run_date,
        args.run_id,
    )

    safe_run(
        "Transform Silver",
        transform_silver,
        spark,
        os.path.join(bronze_output_base, "transactions"),
        os.path.join(bronze_output_base, "merchants"),
        silver_output_base,
        args.run_date,
    )

    safe_run(
        "Build Gold",
        run_gold,
        spark,
        silver_output_base,
        gold_output_base,
        args.run_date,
    )

    run_metadata = {
        "run_date": args.run_date,
        "run_id": args.run_id,
        "status": "SUCCESS",
        "pipeline": "fixed_pipeline",
    }
    metadata_path = f"{metadata_output_base.rstrip('/')}/run_metadata_{args.run_date}.json"
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(run_metadata, f, indent=2)
    print(f"[OUTPUT] Pipeline metadata saved to {metadata_path}")


if __name__ == "__main__":
    main()
