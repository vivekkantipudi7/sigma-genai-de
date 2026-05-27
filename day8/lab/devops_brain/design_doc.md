# Data Pipeline Design Document

## What This Pipeline Does
This pipeline ingests transaction data from both clean and dirty sources, processes it, and stores it in a DuckDB database. It transforms raw transaction data into enriched, high-quality datasets and computes merchant performance metrics and daily summaries.

## Data Flow Diagram
```plaintext
+--------------------+     +------------------+     +-------------------+     +-------------------+
|  Source            |     |  Bronze Layer    |     |  Silver Layer     |     |  Gold Layer       |
|  (TRANSACTIONS)   |<----| (bronze_transactions) <----| (silver_transactions) <----| (gold_merchant_performance, gold_daily_summary) |
+--------------------+     +------------------+     +-------------------+     +-------------------+
                  |                     |                       |                           |
                  |                     |                       |                           |
                  v                     v                       v                           v
+-----------------------+     +----------------------+     +----------------------+     +--------------------+
| Load Merchants       |     | Load Bronze           |     | Transform Bronze to  |     | Compute Metrics    |
| (merchants)           |     | (all_transactions)    |     | Silver               |     | (merchant_perf,    |
+-----------------------+     +----------------------+     +----------------------+     +--------------------+
                  |                     |                       |                           |
                  |                     |                       |                           |
                  v                     v                       v                           v
+-----------------------+     +----------------------+     +----------------------+     +--------------------+
| Insert Merchants     |     | Insert Transactions  |     | Insert Silver        |     | Insert Gold        |
| (merchants)          |     | (bronze_transactions)|     | (silver_transactions)|     | (gold_merchant_performance, |
+-----------------------+     +----------------------+     +----------------------+     +--------------------+
```

## Key Design Decisions
- **Layered Approach:** The pipeline uses a three-layer approach (Bronze, Silver, Gold) to ensure data quality and enrichment.
- **Data Enrichment:** The Silver layer enriches transaction data by joining it with merchant information.
- **Aggregation:** The Gold layer aggregates data to provide merchant performance metrics and daily summaries.
- **Error Handling:** The pipeline includes error handling for missing merchant data and duplicate transactions.

## Known Limitations
- **Data Quality:** The pipeline assumes that the `TRANSACTIONS_CLEAN` and `TRANSACTIONS_DIRTY` data sources are correctly formatted.
- **Performance:** The pipeline may experience performance issues with very large datasets due to in-memory transformations.
- **Data Consistency:** There is no mechanism for handling real-time updates or incremental loads.
- **Error Logging:** The pipeline does not log errors beyond basic print statements.

## Dependencies
- **DuckDB Database:** The pipeline requires a DuckDB database to store the data.
- **MERCHANTS Data:** A predefined list of merchants is required for enriching transaction data.
- **TRANSACTIONS_CLEAN and TRANSACTIONS_DIRTY:** The pipeline depends on these data sources for raw transaction data.