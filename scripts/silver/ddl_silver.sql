/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


if object_id('silver_crm_cust_info','u') is not null
drop table silver_crm_info;

Create table silver_crm_cust_info(
cst_id int ,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date Date,
dwh_create_date datetime2 default  getdate()
);

if object_id('silver_crm_pre_info','u') is not null
drop table silver_crm_pre_info;

Create table silver_crm_pre_info(
prd_id int,
cat_id  nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default  getdate()

);

if object_id('silver_crm_Sales_details','u') is not null
drop table silver_crm_Sales_details;

Create table silver_crm_Sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int ,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default  getdate()

);

if object_id('silver_erp_loc_a101','u') is not null
drop table silver_erp_loc_a101;

Create table silver_erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date datetime2 default  getdate()

);

if object_id('silver_erp_cust_az12','u') is not null
drop table silver_erp_cust_az12;

Create table silver_erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50),
dwh_create_date datetime2 default  getdate()
);

if object_id('silver_erp_px_cat_g1v2','u') is not null
drop table silver_erp_px_cat_g1v2;

Create table silver_erp_px_cat_g1v2(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(50),
dwh_create_date datetime2 default  getdate()
)
