INSERT INTO silver.crm_cust_info (
    cust_id,
    cust_key,
    cust_firstname,
    cust_lastname,
    cust_marital_status,
    cust_gndr,
    cust_create_date
)
SELECT 
    cust_id,
    cust_key,
    TRIM(cust_firstname) cust_firstname,
    TRIM(cust_lastname) cust_lastname,
    CASE 
        WHEN UPPER(cust_marital_status) = 'M' THEN 'Married'
        WHEN UPPER(cust_marital_status) = 'S' THEN 'Single'
        WHEN UPPER(cust_marital_status) = 'D' THEN 'Divorced'
        WHEN UPPER(cust_marital_status) = 'W' THEN 'Widowed'
        ELSE 'Unknown'
    END AS cust_marital_status,
    CASE 
        WHEN UPPER(TRIM(cust_gndr)) = 'M' THEN  'Male'
        WHEN UPPER(TRIM(cust_gndr)) = 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS cust_gndr,
    cust_create_date
FROM (SELECT *,
ROW_NUMBER () OVER(PARTITION BY cust_id ORDER BY cust_create_date DESC) flag_dup
FROM bronze.crm_cust_info
WHERE cust_id IS NOT NULL)
WHERE flag_dup = 1 

SELECT COUNT(*) FROM silver.crm_cust_info

INSERT INTO silver.crm_prd_info (  
    prd_id,
    prd_key,
    cat_id,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_date,
    prd_end_date)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') cat_id,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) prd_key,
    TRIM(prd_nm) prd_nm,
    COALESCE(prd_cost, 0) prd_cost,
    CASE UPPER(TRIM(prd_line)) 
        WHEN 'M' THEN 'Mountain'
        WHEN 'R' THEN 'Road'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'T' THEN 'Touring'
        ELSE 'Unknown'
    END AS prd_line,
    prd_start_date,
    LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) - 1 AS prd_end_date
FROM
    bronze.crm_prd_info
SELECT * FROM silver.crm_prd_info
