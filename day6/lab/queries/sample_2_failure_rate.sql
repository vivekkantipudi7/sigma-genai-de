-- Sample Query 2: Transaction failure rate by payment method
SELECT 
    payment_method,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_count,
    ROUND(100.0 * SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) / COUNT(*), 2) as failure_rate_pct
FROM fact_transactions
GROUP BY payment_method
ORDER BY failure_rate_pct DESC;
