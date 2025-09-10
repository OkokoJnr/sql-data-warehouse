--This scripts integrate data into the bronze layers
CREATE OR REPLACE PROCEDURE load_bronze()
LANGUAGE plpgsql

AS $$ 
DECLARE
    start_time TIMESTAMP; 
    end_time TIMESTAMP;
    exec_time NUMERIC;

    
BEGIN
start_time := clock_timestamp();
RAISE NOTICE 'Executing the bronze layer';
RAISE NOTICE 'Loading data into: crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    copy bronze.crm_cust_info FROM 'C:\Program Files\PostgreSQL\17\datasets\source_crm\cust_info.csv' WITH (FORMAT csv, HEADER true);

RAISE NOTICE 'crm_cust_info loaded. Now Loading data into: crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    copy bronze.crm_prd_info FROM 'C:\Program Files\PostgreSQL\17\datasets\source_crm\prd_info.csv' WITH (FORMAT csv, HEADER true);


RAISE NOTICE 'crm_prd_info loaded. Now Loading data into: crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    copy bronze.crm_sales_details FROM 'C:\Program Files\PostgreSQL\17\datasets\source_crm\sales_details.csv' WITH (FORMAT csv, HEADER true);



RAISE NOTICE 'crm_sales_details loaded. Now Loading data into: erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    copy bronze.erp_px_cat_g1v2 FROM 'C:\Program Files\PostgreSQL\17\datasets\source_erp\px_cat_g1v2.csv' WITH (FORMAT csv, HEADER true);


RAISE NOTICE 'erp_px_cat_g1v2 loaded. Now Loading data into: erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;
    copy bronze.erp_cust_az12 FROM 'C:\Program Files\PostgreSQL\17/datasets\source_erp\cust_az12.csv' WITH (FORMAT csv, HEADER true);

RAISE NOTICE 'erp_cust_az12 loaded. Now Loading data into: erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;
    copy bronze.erp_loc_a101 FROM 'C:\Program Files\PostgreSQL\17\datasets\source_erp\loc_a101.csv' WITH (FORMAT csv, HEADER true);
end_time := clock_timestamp();
exec_time := EXTRACT(EPOCH FROM (end_time - start_time)); 
RAISE NOTICE 'Bronze layer executed successfully % seconds', exec_time;
EXCEPTION
WHEN others THEN
RAISE NOTICE 'An unexpected error occured: %', SQLERRM;
END;
$$;

    -- End of file: scripts/load.sql


