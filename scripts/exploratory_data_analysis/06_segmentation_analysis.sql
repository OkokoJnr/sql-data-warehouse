/*
Segment products into cost ranges and count how many products fall into each segment
*/

WITH product_segment AS (SELECT
    product_key,
    product_name,
    cost,
    CASE
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 and 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'ABOVE 1K'
    END cost_segment
FROM gold.dim_products)

SELECT 
    cost_segment,
        COUNT(product_key) AS total_product
FROM product_segment
GROUP BY cost_segment
ORDER BY total_product DESC

/*
Group customers into three segments on their spending behaviour:
    -VIP: Customers with at least 12 months of history and spending more than 5000
    - Regular: Customers with at least 12 months of istory but spending 5000 or less
    - New: Customers with a lifespan less tan 12 months
    And find the total number of customers by each group
*/

WITH customer_segments AS (SELECT
    c.customer_id,
    SUM(s.sales_amount) AS total_sales,
    MAX(order_date) AS first_order_date,
    MIN(order_date) AS last_order_date,
    (EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
    (EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS lifespan
FROM gold.fact_sales s
LEFT JOIN gold.dim_customers c
ON s.customer_key = c.customer_key
WHERE order_date IS NOT NULL
GROUP BY c.customer_id)

SELECT 
    customer_segment,
    COUNT(customer_id)
FROM(
SELECT
    customer_id,
    CASE 
        WHEN lifespan >= 12 AND total_sales > 5000 THEN 'VIP'
        WHEN lifespan >= 12 AND total_sales <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS customer_segment
FROM customer_segments)
GROUP BY customer_segment