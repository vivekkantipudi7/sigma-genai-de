-- Customer Revenue Analysis Query
-- Bug 1 (Correctness): Revenue includes FAILED/PENDING transactions (should only count COMPLETED)
-- Bug 2 (Performance): Correlated subquery instead of JOIN + no LIMIT on window function results
-- Bug 3 (Security): Unnecessarily exposes customer email
-- Bug 4 (Readability): No table aliases on subquery, unclear variable names

SELECT 
    ft.transaction_id,
    ft.amount,
    ft.status,
    (SELECT email FROM sigma_analytics.dim_customer dc WHERE dc.customer_id = ft.customer_id) AS customer_email,
    SUM(ft.amount) OVER (PARTITION BY ft.customer_id) AS customer_lifetime_value,
    dm.merchant_name
FROM sigma_analytics.fact_transactions ft
JOIN sigma_analytics.dim_merchant dm ON ft.merchant_id = dm.merchant_id
ORDER BY customer_lifetime_value DESC;
