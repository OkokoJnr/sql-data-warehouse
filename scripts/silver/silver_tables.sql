--This script creates all tables for the silver layers

DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cust_id INT,
    cust_key VARCHAR(50),
    cust_firstname VARCHAR(50),
    cust_lastname VARCHAR(50),
    cust_marital_status VARCHAR(50),
    cust_gndr VARCHAR(50),
    cust_create_date DATE 
);

DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost FLOAT,
    prd_line VARCHAR(50),
    prd_start_date DATE,
    prd_end_date DATE,
    dwh_create_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales FLOAT,
    sls_quantity INT,
    sls_price FLOAT,
    dwh_create_date DATE DEFAULT NOW()
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2(
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenace VARCHAR(50),
    dwh_create_date DATE DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
    cid VARCHAR(50),
    bdate VARCHAR(50),
    gen VARCHAR(50),
    dwh_create_date DATE DEFAULT NOW()
);


DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
    cid VARCHAR(50),
    cntry VARCHAR(50),
    dwh_create_date DATE DEFAULT NOW()
);

