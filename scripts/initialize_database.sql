/* 
=====================================
Create Database and Schemas
=====================================
Script Purpose: 
This script creates a new database nmaed 'DataWarehouse'. The script also sets up three schemas within the database: 
'bronze', 'silver', and 'gold.
*/


Use master;

--Create the database 'DataWarehouse'
Create DATABASE DataWarehouse

Use DataWarehouse

--Creating Schema Layers

Create SCHEMA bronze
Create Schema silver 
Create SCHEMA gold
