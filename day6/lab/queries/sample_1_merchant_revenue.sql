-- Sample Query 1: Total revenue by merchant
SELECT 
    m.merchant_name,
    SUM(CASE WHEN t.status = 'COMPLETED' THEN t.amount ELSE 0 END) AS revenue
FROM fact_transactions t
JOIN dim_merchant m ON t.merchant_id = m.merchant_id
GROUP BY m.merchant_name
ORDER BY revenue DESC;
