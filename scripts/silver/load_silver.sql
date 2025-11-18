/*
-------------------------------------------------
Load Silver Layer (Bronze -> Silver) 
-------------------------------------------------
Script Purpose: 
  This script performs the ETL (extract, transform, load) process to populate the 'silver' schema tables
  from the 'bronze' schema.
Actions Performed: 
  -Truncate Silver tables 
  -Insert transformed and cleaned data from Bronze to Silver tables
-------------------------------------------------------------------------------------------------------
*/

TRUNCATE TABLE silver.crm_cust_info --keeps from adding duplicate records if insert statement is ran again
--Inserting transformed data into silver.cust_info table 
INSERT INTO silver.crm_cust_info (
    cst_id, 
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_gndr,
    cst_marital_status,
    cst_create_date)
--Performing data transformations on bronze.crm_cust_info table
SELECT cst_id, 
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
   CASE WHEN UPPER(Trim(cst_gndr)) = 'F' THEN 'Female'
   WHEN UPPER(Trim(cst_gndr)) = 'M' THEN 'Male'
   Else 'n/a'
END AS cst_gndr, --normalize gender value to readable format
CASE WHEN UPPER(Trim(cst_marital_status)) = 'S' THEN 'Single'
   WHEN UPPER(Trim(cst_marital_status)) = 'M' THEN 'Married'
   Else 'n/a'
END as cst_marital_status,
cst_create_date
FROM (
    SELECT *, ROW_NUMBER() OVER(partition BY cst_id ORDER BY cst_create_date DESC) as flag_last --removing duplicates
    FROM bronze.crm_cust_info
     WHERE cst_id IS NOT NULL
     ) as t 
     WHERE flag_last = 1 --filtering the data by keeping only the latest record called flag_last

TRUNCATE TABLE silver.crm_prd_info --keeps from records being duplicated if insert statement is ran again
--Inserting transformed data into silver.crm_prd_info table
INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt)
--Performing data transformations on bronze.crm_prd_info table
SELECT prd_id, 
Replace(Substring(prd_key, 1, 5), '-', '_') as cat_id, 
Substring(prd_key, 7, Len(prd_key)) as prd_key,
prd_nm, 
ISNull(prd_cost, 0) as prd_cost,
Case Upper(Trim(prd_line))
    When 'M' THEN 'Mountain'
    When 'R' THEN 'Road'
    When 'S' THEN 'Other Sales'
    When 'T' THEN 'Touring' 
    Else 'n/a'
End as prd_line,
prd_start_dt, 
Lead(prd_start_dt) OVER (partition by prd_key ORDER BY prd_start_dt) As prd_end_dt --window function to transform start date so it makes sense
From bronze.crm_prd_info

TRUNCATE TABLE silver.crm_sales_details --keeps from records being duplicated if insert statement is ran again 
--Inserting transformed data into silver.crm_sales_details table
INSERT INTO silver.crm_sales_details(
    sls_ord_num,
   sls_prd_key,
   sls_cust_id,
   sls_order_dt,
   sls_ship_dt,
   sls_due_date,
   sls_sales,
   sls_quantity,
   sls_price
)
--Transforming data in bronze.crm_sales_details table
Select 
   sls_ord_num,
   sls_prd_key ,
   sls_cust_id,
CASE WHEN sls_order_dt = 0 OR  Len(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt As VARCHAR) As DATE)    --data type casting: tranforming sls_order_dt int to date (must be converted to varchar first then date)
    END as sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR  Len(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt As VARCHAR) As DATE)    
    END as sls_ship_dt,
CASE WHEN sls_due_date = 0 OR  Len(sls_due_date) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_date As VARCHAR) As DATE)  
    End as sls_due_date,  
Case WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    Else sls_sales
    End AS sls_sales,
sls_quantity,
CASE When sls_price IS NULL OR sls_price <= 0
    THEN sls_sales / NULLIF(sls_quantity, 0)
    ELSE sls_price
END as sls_price
From bronze.crm_sales_details


TRUNCATE TABLE silver.erp_cust_az12 --keep from records duplicating if insert statement is ran again 
--Inserting transformed data into silver.erp_cust_az12 table
Insert into silver.erp_cust_az12 (cust_cid, bdate, gen
)
--Transforming data from bronze.erp_cust_az12 table
Select 
Case When cust_cid LIKE 'NAS%' THEN SUBSTRING(cust_cid, 4, Len(cust_cid))
    ELSE cust_cid
End as cust_cid,
Case When bdate > GETDATE() THEN NULL
    ELSE bdate 
END as bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
    When UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male'
    ELSE 'n/a'
End as gen
From bronze.erp_cust_az12


TRUNCATE TABLE silver.erp_loc_a101 --keeps from records duplicating if insert statement is run again
--Insert transformed data into silver.erp_loc_a101 table
Insert Into silver.erp_loc_a101(
    cid, cntry
)
--Transforming data from bronze.erp_loc_a101 table
Select Replace(cid, '-', '') cid,
Case when Trim(cntry) in ('US', 'USA') THEN 'United States'
    WHEN Trim(cntry) = 'DE' THEN 'Germany'
    When trim(cntry) = '' Then 'n/a'
    ELSE trim(cntry)
END as cntry
From bronze.erp_loc_a101

TRUNCATE TABLE silver.erp_pd_cat_g1v2 --keep from records duplicating if insert statement is run again 
--Insert into silver.erp_pd_cat_g1v2 table
Insert Into silver.erp_pd_cat_g1v2(
    id,cat, subcat, maintenance
)
--Transforming data from bronze.erp_pd_cat_g1v2 table
Select id,
cat,
subcat,
maintenance
From bronze.erp_pd_cat_g1v2
