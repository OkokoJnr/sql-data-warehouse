/*
STORED PROCEDURE: LOAD SILVER LAYER (BRONZE => LAYER)
    SCRIPTS PURPOSE:
        This stored procedure performes ETL (Extract, Transform, Load) process to populate the silver schema tables from the bronze schema
    ACTIONS PERFORMED:
        * Truncate silver tables
        * Inserts transformed and cleaned data from bronze into silver
    PARAMETERS:
        * None
        This stored procedure does not accept any parameters or return any values.

    USAGE EXAMPLES
        CALL silver.load_silver()
*/
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql

AS $$ 
DECLARE
    start_time TIMESTAMP; 
    end_time TIMESTAMP;
    exec_time NUMERIC;

BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE 'Executing the silver layer';
        TRUNCATE TABLE silver.crm_cust_info;
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
        WHERE flag_dup = 1;

        --SELECT COUNT(*) FROM silver.crm_cust_info

        TRUNCATE TABLE silver.crm_prd_info;
        INSERT INTO silver.crm_prd_info (  
            prd_id,
            cat_id,
            prd_key,
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
            bronze.crm_prd_info;


        TRUNCATE TABLE  silver.crm_sales_details;
        INSERT INTO silver.crm_sales_details(
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE 
                WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt:: TEXT) <> 8 THEN NULL
                ELSE CAST(sls_order_dt AS TEXT):: DATE
            END AS sls_order_dt,
            
            CASE 
                WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt:: TEXT) <> 8  THEN NULL
                ELSE CAST(sls_ship_dt AS TEXT):: DATE
            END AS sls_ship_dt,
            
            CASE 
                WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt:: TEXT) <> 8 THEN NULL
                ELSE CAST(sls_due_dt AS TEXT):: DATE
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales <> sls_quantity *sls_price THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            CASE 
                WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
                WHEN sls_price < 0 THEN ABS(sls_price)
                ELSE sls_price
            END AS sls_price,

            CASE 
                WHEN sls_quantity <= 0 OR sls_quantity IS NULL THEN sls_sales / NULLIF(sls_price, 0)
                ELSE sls_quantity
            END AS sls_quantity
        FROM bronze.crm_sales_details;

        --SELECT COUNT(*) FROM bronze.crm_sales_details


        TRUNCATE TABLE  silver.erp_cust_az12;
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN UPPER(cid) LIKE 'NASA%' THEN SUBSTRING(cid, 4, LENGTH(cid))
                ELSE cid
            END cid,
            CASE 
                WHEN bdate::DATE > NOW() THEN NULL
                ELSE bdate
            END bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            ELSE 'Uknown'
            END AS gen 
        FROM bronze.erp_cust_az12;

        TRUNCATE TABLE  silver.erp_loc_a101;
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )SELECT
            REPLACE(cid, '-', '') cid,
            CASE 
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
            ELSE cntry
            END AS cntry
        FROM bronze.erp_loc_a101;

        TRUNCATE TABLE  silver.erp_px_cat_g1v2;
        INSERT INTO silver.erp_px_cat_g1v2(
            id, 
            cat,
            subcat,
            maintenace
        )
        SELECT 
            id, 
            cat,
            subcat,
            maintenace
        FROM bronze.erp_px_cat_g1v2;

        --SELECT * FROM silver.erp_px_cat_g1v2
        RAISE NOTICE 'All dataset load sucessfully';
        end_time := clock_timestamp();
        exec_time := EXTRACT(EPOCH FROM (end_time - start_time)); 
        RAISE NOTICE 'Silver layer executed successfully % seconds', exec_time;
        EXCEPTION
            WHEN others THEN
                RAISE NOTICE 'An unexpected error occured: %', SQLERRM; 
END;
$$;


--CALL load_bronze()