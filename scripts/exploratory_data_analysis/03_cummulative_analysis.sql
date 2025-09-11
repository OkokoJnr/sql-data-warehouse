--cummulative aggregation
--Calculate the total sales per month and the running total of sales over time


SELECT 
    order_month,
    total_sales,
    quantity ,
    SUM(total_sales) OVER(PARTITION BY DATE_TRUNC('year', order_month) ORDER BY order_month) running_total
FROM (
    SELECT
        DATE_TRUNC('month', order_date)::date order_month,
        SUM(quantity) AS quantity,
        SUM(sales_amount) AS total_sales
    FROM gold.fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY DATE_TRUNC('month', order_date)) t
