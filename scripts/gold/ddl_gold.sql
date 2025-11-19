/*
=================================================================
DDL Script: Create Gold Views 
==================================================================
Script Purpose: 
  This script creates views for the Gold Layer in the data warehouse. 
  The Gold layer represents the final dimension and fact tables (Star Schema).

  Each view performs transformations and combines data from the Silver layer 
  to produce a clean, enriched, business-ready dataset. 

Usage:
  These views can be queried directly for analysis and reporting. 
===================================================================
*/

--==================================================================
--Create Dimension: gold.dim_customers 
--==================================================================
If OBJECT_ID('gold.dim_customers', 'v') IS NOT NULL
    DROP VIEW gold.dim_customers
    GO
--Building view
Create VIEW gold.dim_customers AS
Select 
    Row_number() over(ORDER BY cst_id) as customer_key, --generating surrogate primary key for dim table
    ci.cst_id as customer_id, 
    ci.cst_key as customer_number, 
    ci.cst_firstname as first_name, 
    ci.cst_lastname as last_name, 
    la.cntry as country,
    ci.cst_marital_status as marital_status,  
CASE WHEN ci.cst_gndr != 'n/a' Then ci.cst_gndr --CRM is the master source for gender
    ELSE Coalesce(ca.gen, 'n/a')
End as gender,
    ca.bdate as birthdate, 
    ci.cst_create_date as create_date
    from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
On ci.cst_key = ca.cust_cid 
Left join silver.erp_loc_a101 la
on ci.cst_key = la.cid


--==================================================================
--Create Dimension: gold.dim_products
--==================================================================
If OBJECT_ID('gold.dim_products', 'v') IS NOT NULL
    DROP VIEW gold.dim_products
    GO
--Building view
Create VIEW gold.dim_products AS
Select 
    ROW_number() OVER(Order by pn.prd_start_dt, pn.prd_key) as product_key, --generating surrogate pk for dim table
    pn.prd_id as product_id,
    pn.prd_key as product_number, 
    pn.prd_nm as product_name,
    pn.cat_id as category_id,  
    pc.cat as category, 
    pc.subcat as subcategory, 
    pc.maintenance,
    pn.prd_cost as cost, 
    pn.prd_line as product_line, 
    pn.prd_start_dt as start_date
From silver.crm_prd_info pn
Left JOIN silver.erp_pd_cat_g1v2 pc
On pn.cat_id = pc.id 
Where prd_end_dt IS Null --filtering out the oldest records (keeping only end dates where null)


--==================================================================
--Create Fact: gold.fact_sales
--==================================================================
If OBJECT_ID('gold.fact_sales', 'v') IS NOT NULL
    DROP VIEW gold.fact_sales
    GO
--Buidling view
Create VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num as order_numnber,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_date as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd.sls_price as price
From silver.crm_sales_details sd 
Left Join gold.dim_products pr 
On sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu 
On sd.sls_cust_id = cu.customer_id

