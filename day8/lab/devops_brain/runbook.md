# Pipeline Overview

This pipeline processes transaction data, transforms it, and loads it into bronze, silver, and gold tables. It runs to ensure data is available for reporting and analytics. If it stops, downstream reports and dashboards will be incomplete or outdated.

## Pipeline Steps

1. Connect to the DuckDB database using `get_connection()`.
2. Set up necessary tables using `setup_tables(con)`.
3. Load merchant data into the `merchants` table using `load_merchants(con)`.
4. Load transactions into the `bronze_transactions` table using `load_bronze(con, transactions)`.
5. Transform bronze transactions to silver using `transform_bronze_to_silver(transactions, merchants)`.
6. Load transformed data into the `silver_transactions` table using `load_silver(con, silver_rows)`.
7. Compute merchant performance metrics using `compute_merchant_performance(silver_rows)`.
8. Compute daily summary metrics using `compute_daily_summary(silver_rows)`.
9. Load performance and summary data into the `gold_merchant_performance` and `gold_daily_summary` tables using `load_gold(con, merchant_perf, daily_summary)`.

## Schedule / Trigger

This pipeline runs every night at midnight using a cron job.

## Failure Modes

1. **Database Connection Failure**
   - *Root Cause*: DuckDB service is down.
   - *Symptom*: `get_connection()` fails.
2. **Table Creation Failure**
   - *Root Cause*: Syntax error in SQL.
   - *Symptom*: `setup_tables(con)` raises an exception.
3. **Merchant Data Load Failure**
   - *Root Cause*: Corrupt merchant data.
   - *Symptom*: `load_merchants(con)` fails.
4. **Bronze Table Load Failure**
   - *Root Cause*: Invalid transaction data.
   - *Symptom*: `load_bronze(con, transactions)` fails.
5. **Silver Transformation Failure**
   - *Root Cause*: Missing merchant mapping.
   - *Symptom*: `transform_bronze_to_silver(transactions, merchants)` fails.

## Recovery Actions

1. **Database Connection Failure**
   - Check DuckDB service status.
   - Restart the service if necessary.
   - Retry the pipeline.
2. **Table Creation Failure**
   - Review SQL syntax in `setup_tables(con)`.
   - Correct the syntax and retry.
3. **Merchant Data Load Failure**
   - Validate merchant data for corruption.
   - Clean the data and retry `load_merchants(con)`.
4. **Bronze Table Load Failure**
   - Inspect transaction data for validity.
   - Clean or filter out invalid data and retry `load_bronze(con, transactions)`.
5. **Silver Transformation Failure**
   - Ensure all merchants have valid mappings.
   - Update merchant data and retry `transform_bronze_to_silver(transactions, merchants)`.

## Known Bugs

- Hardcoded AWS credentials in the code.
- Lack of null handling in `transform_bronze_to_silver(transactions, merchants)`.

## Escalation Contacts

1. **On-call DE**: Priya Nair (priya.nair@sigmadatatech.in, +91-98400-11111)
2. **Tech Lead**: Arjun Mehta (arjun.mehta@sigmadatatech.in)
3. **Platform Manager**: Kavya Reddy (kavya.reddy@sigmadatatech.in)

## Data Quality Checks

- Verify the count of records in `bronze_transactions`, `silver_transactions`, `gold_merchant_performance`, and `gold_daily_summary`.
- Ensure `quality_flag` is correctly set in `silver_transactions`.
- Check for any NULL values in critical fields.
- Validate the computed metrics in `gold_merchant_performance` and `gold_daily_summary`.