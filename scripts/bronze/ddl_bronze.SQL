/*
========================================
DDL Script: Create Bronze Tables
========================================
Script Purpose: 
  This script creates tables in the 'bronze' schema while dropping existing tables if they already exist. Run this script to redefine the structure of the 'bronze' tables. 
  */

--Creating bronze tables (using CRM & ERP source files)
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info (
   cst_id INT,
   cst_key NVARCHAR (50),
   cst_firstname NVARCHAR (50),
   cst_lastname NVARCHAR (50),
   cst_marital_status NVARCHAR (50),
   cst_gndr NVARCHAR (50),
   cst_create_date DATE,
)

IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
   DROP TABLE bronze.crm_prd_info 
CREATE TABLE bronze.crm_prd_info (
   prd_id INT,
   prd_key NVARCHAR (50),
   prd_nm NVARCHAR (50),
   pr_cost NVARCHAR (50),
   prd_line NVARCHAR (50),
   prd_start_dt DATE,
   prd_end_dt DATE,
)

IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
   DROP TABLE bronze.crm_sales_details 
Create TABLE bronze.crm_sales_details (
   sls_ord_num NVARCHAR (50),
   sls_prd_key NVARCHAR (50),
   sls_cust_id INT,
   sls_order_dt INT,
   sls_ship_dt INT,
   sls_due_date INT,
   sls_sales INT,
   sls_quantity INT,
   sls_price int,
)

IF OBJECT_ID ('bronze.erp_pd_cat_g1v2', 'U') IS NOT NULL
   DROP TABLE bronze.erp_pd_cat_g1v2 
Create table bronze.erp_pd_cat_g1v2 (
   id NVARCHAR(50),
   cat NVARCHAR(50),
   subcat NVARCHAR(50),
   maintenance NVARCHAR (50),
)

IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
   DROP TABLE bronze.erp_loc_a101 
Create TABLE bronze.erp_loc_a101 (
   cid NVARCHAR(50),
   cntry NVARCHAR(50),
)

IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
   DROP TABLE bronze.erp_cust_az12
Create TABLE bronze.erp_cust_az12 (
   cust_cid NVARCHAR (50),
   bdate DATE,
   gen NVARCHAR(50),
)

--Performing Bulk Insert to add data to tables
Bulk INSERT bronze.crm_cust_info
From '/data/cust_info.csv'
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
