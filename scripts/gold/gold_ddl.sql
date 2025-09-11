/*
DDL SCRIPTS: CREATE GOLD VIEWS

    SCRIPT PURPOSE:
        This scripts creates views for the gold layer in the data wareouse. The Gold layer represent the final dimension and fact tables (star schema)

        Each view performs transformations and combines data from the silver layer to produce a clean, enriched, and business-ready dataset

    USAGE:
        These views can be queried directly for analytics and reporting
*/
DROP VIEW IF EXISTS gold.dim_customers CASCADE;
CREATE VIEW gold.dim_customers AS SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cust_id ASC) customer_key,
    ci.cust_id AS customer_id,
    ci.cust_key customer_number,
    ci.cust_firstname AS first_name,
    ci.cust_lastname AS last_name,
    la.cntry AS country,
    ci.cust_marital_status AS marital_status,  
       CASE WHEN ci.cust_gndr <> 'Unknown' THEN ci.cust_gndr
    ELSE COALESCE(ca.gen, 'n/a')
    END gender,    --CRM is the master table for gender info
    ca.bdate AS birthday,
    ci.cust_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON  ci.cust_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON  ci.cust_key = la.cid


DROP VIEW IF EXISTS gold.dim_products CASCADE;
CREATE VIEW gold.dim_products AS (SELECT 
    ROW_NUMBER() OVER(ORDER BY pi.prd_id) product_key,
    pi.prd_id AS product_id,
    pi.prd_key AS product_number,
    pi.prd_nm AS product_name,
    pi.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS sub_category,
    pc.maintenace,
    pi.prd_line product_line,
    pi.prd_cost AS cost,
    pi.prd_start_date AS start_date 
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pi.cat_id = pc.id
WHERE prd_end_date IS NULL)


DROP VIEW IF EXISTS gold.fact_sales CASCADE;
CREATE VIEW gold.fact_sales AS 
SELECT
    sls_ord_num AS order_number,
    dp.product_key,
    dc.customer_key,
    sls_order_dt AS order_date,
    sls_ship_dt AS shipping_date,
    sls_due_dt AS due_date,
    sls_sales AS sales_amount,
    sls_quantity AS quantity,
    sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers dc
ON  sd.sls_cust_id = dc.customer_id
LEFT JOIN gold.dim_products dp
ON  sd.sls_prd_key = dp.product_number


SELECT * FROM gold.dim_customers