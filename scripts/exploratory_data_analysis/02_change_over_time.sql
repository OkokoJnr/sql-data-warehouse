--changes over time
SELECT
    TO_CHAR(order_date, 'yyyy') AS year,

    TO_CHAR(order_date, 'Month') AS month,
    SUM( sales_amount) AS total_sales,

    COUNT(DISTINCT customer_key) total_customers,
    SUM(quantity)  AS total_quantity_sold,
    
    SUM(SUM(sales_amount)) OVER(PARTITION BY TO_CHAR(order_date, 'yyyy')) AS yearly_sales
FROM
    gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY TO_CHAR(order_date, 'yyyy'), TO_CHAR(order_date, 'Month')
ORDER BY total_sales DESC;



