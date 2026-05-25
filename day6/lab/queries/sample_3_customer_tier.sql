-- Sample Query 3: Customer transaction counts by tier
SELECT 
    dc.tier,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT dc.customer_id) as unique_customers,
    AVG(ft.amount) as avg_transaction_amount
FROM fact_transactions ft
JOIN dim_customer dc ON ft.customer_id = dc.customer_id
WHERE ft.status = 'COMPLETED'
GROUP BY dc.tier
ORDER BY transaction_count DESC;
