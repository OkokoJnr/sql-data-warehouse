    --performance analysis
/*
Analyze the yearly performance of products by comparing their sales to both the avearge sales performance of the product and the previous year's sales
*/

WITH yearly_product_sales AS (SELECT 
    DATE_TRUNC('year', order_date)::DATE AS order_year,
    dp.product_name,
    SUM(fs.sales_amount) AS current_sales
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON   fs.product_key = dp.product_key
WHERE  order_date IS NOT NULL
GROUP BY  DATE_TRUNC('year', order_date), dp.product_name
ORDER BY  DATE_TRUNC('year', order_date))

SELECT
    order_year,
    product_name,
    current_sales,
    AVG(current_sales) OVER(PARTITION BY product_name) AS avg_yearly_sales,
    current_sales - AVG(current_sales) OVER(PARTITION BY product_name) diff_avg,
    CASE 
        WHEN (current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) > 0 THEN 'Above '
        WHEN (current_sales - AVG(current_sales) OVER(PARTITION BY product_name)) < 0 THEN 'Below '
        ELSE 'Equal to Average'
    END flag_avg,
    LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS prev_year_sales,
    current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py,
    CASE
        WHEN ( current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) > 0 THEN 'Increase'
        WHEN ( current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year)) < 0 THEN 'Decrease'
        ELSE 'No Difference'
    END flag_sales
FROM
    yearly_product_sales
ORDER BY product_name, order_year