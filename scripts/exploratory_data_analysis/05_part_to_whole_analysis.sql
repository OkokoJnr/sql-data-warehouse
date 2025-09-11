
--part-to-whole analysis
SELECT
    p.category,
    SUM(f.sales_amount) AS total_sales_category,
    (ROUND(
        (SUM(f.sales_amount)::NUMERIC *100 /SUM(SUM(f.sales_amount)::NUMERIC) OVER()),
        2))::TEXT || '%' AS perc_contribution
FROM
    gold.fact_sales f
LEFT JOIN gold.dim_products p
ON  p.product_key = f.product_key
GROUP BY p.category

