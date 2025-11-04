/*
========================================
DDL Script: Create Bronze Tables
========================================
Script Purpose: 
  This script creates tables in the 'bronze' schema. Run this script to redefine the structure of the 'bronze' tables. 
  */





--Creating tables for bronze layer (using CRM & ERP source files)
CREATE TABLE bronze.crm_cust_info (
    cst_id INT, 
    cst_key NVARCHAR (50),
    cst_firstname NVARCHAR (50),
    cst_lastname NVARCHAR (50),
    cst_material_status NVARCHAR (50),
    cst_gndr NVARCHAR (50),
    cst_create_date DATE
)


CREATE TABLE bronze.crm_prd_info (
    prd_id INT, 
    prd_key NVARCHAR (50), 
    prd_nm NVARCHAR (50), 
    pr_cost NVARCHAR (50), 
    prd_line NVARCHAR (50), 
    prd_start_dt DATE, 
    prd_end_dt DATE
)

Create TABLE bronze.crm_sales_details (
    sls_ord_num NVARCHAR (50),
    sls_prd_key NVARCHAR (50), 
    sls_cust_id INT, 
    sls_order_dt INT, 
    sls_ship_dt INT, 
    sls_due_date INT, 
    sls_sales INT, 
    sls_quantity INT, 
    sls_price int

)

Create table bronze.erp_pd_cat_g1v2 (
    id NVARCHAR(50), 
    cat NVARCHAR(50), 
    subcat NVARCHAR(50),
    maintenance NVARCHAR (50)
)

Create TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50), 
    cntry NVARCHAR(50)
)

Create TABLE bronze.erp_cust_az12 (
    cust_cid NVARCHAR (50),
    bdate DATE, 
    gen NVARCHAR(50)
)

--Performing Bulk Insert to add data to tables
Bulk INSERT bronze.crm_cust_info 
From '/data/cust_info.csv'

--With clause tells the server how to handle the file: this says our data starts at the 2nd row, how the values are split, and the table will be locked as data is being added. 
With (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)

Bulk INSERT bronze.crm_prd_info
From '/data/prd_info.csv'
With (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)


Bulk INSERT bronze.crm_sales_details 
From '/data/sales_details.csv'
With (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)


Bulk INSERT bronze.erp_cust_az12
From '/data/cust_az12.csv'
With (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)


Bulk INSERT bronze.erp_loc_a101
From '/data/loc_a101.csv'
With (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)


Bulk INSERT bronze.erp_pd_cat_g1v2
From '/data/pd_cat_g1v2.csv'
With (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
)

