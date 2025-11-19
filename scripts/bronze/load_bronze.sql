/*
====================================================
Insert Data from source files into Bronze layer
====================================================
Script Purpose: 
   This script performs a bulk insert of the source csv files into 
   each table created in the Bronze layer. 
=========================================================================
*/

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
