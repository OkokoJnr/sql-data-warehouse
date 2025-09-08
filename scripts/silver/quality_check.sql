                    --crm_cust_info
--CHECKING FOR DUPLICATES AND NULL
SELECT 
    cust_id,
    COUNT(*)
FROM silver.crm_cust_info
GROUP BY cust_id
HAVING COUNT(*) > 1 OR cust_id IS NULL
--CHECK FOR UNWANTED SPACES IN STRING VALUES
SELECT
    cust_firstname,
    cust_lastname,
    cust_marital_status,
    cust_gndr
FROM silver.crm_cust_info
WHERE  cust_firstname <> TRIM(cust_firstname) OR cust_lastname <> TRIM(cust_lastname) OR cust_marital_status <> TRIM (cust_marital_status) OR cust_gndr <> TRIM(cust_gndr)
--DATA STANDARDIZATION AND CONSISTENCY -CHECKING FOR AND STANDARDIZING ABBREVIATION

SELECT DISTINCT cust_marital_status FROM silver.crm_cust_info
SELECT DISTINCT cust_gndr FROM silver.crm_cust_info

--updating data quality
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


--crm_prd_info
    --1. Checking for duplicates or nulls

SELECT 
    COUNT(* )
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--2. Checking for unwanted spaces in string values
SELECT
    prd_key,
    prd_nm,
    prd_line
FROM silver.crm_prd_info
WHERE prd_line <> TRIM(prd_line) OR prd_nm <> TRIM(prd_nm)


--3. Check for nulls or negative values
SELECT
    prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

--4. Data standardization and consistency on the prd_line column
SELECT DISTINCT prd_line FROM silver.crm_prd_info

--checking for invalid dates. (End date must not be earlier than start date)
SELECT
    prd_start_date,
    prd_end_date
FROM silver.crm_prd_info
WHERE prd_end_date < prd_start_date
--Let the end date of a product be the start date -1 of the next product in the same category
SELECT
    prd_key,
    prd_start_date,
    LEAD(prd_start_date) OVER (PARTITION BY prd_key ORDER BY prd_start_date) AS prd_end_date
FROM bronze.crm_prd_info


--updating data quality
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


SELECT * from bronze.crm_prd_info
SELECT * from bronze.erp_px_cat_g1v2