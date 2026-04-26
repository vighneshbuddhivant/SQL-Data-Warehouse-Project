/*
===========================================================
Bronze Layer Table Creation Script (Medallion Architecture)
===========================================================

Purpose:
- Create all Bronze layer tables in a single script
- Bronze layer stores raw data exactly as received from source systems
- No transformations, cleaning, or business logic applied
- Used for ingestion, auditing, and reprocessing

Approach:
- Check if table exists using OBJECT_ID
- Drop table if exists
- Recreate table structure

===========================================================
CRM TABLES (Source: CRM System)
===========================================================

1. bronze.crm_cust_info
- Stores customer master data (raw)
- Includes personal and demographic details

2. bronze.crm_prd_info
- Stores product information
- Includes product lifecycle dates

3. bronze.crm_sales_details
- Stores transactional sales data
- Dates stored as INT (raw format from source)

===========================================================
ERP TABLES (Source: ERP System)
===========================================================

4. bronze.erp_loc_a101
- Stores customer location details

5. bronze.erp_cust_az12
- Stores additional customer attributes

6. bronze.erp_px_cat_g1v2
- Stores product category and classification

===========================================================
Notes:
- Bronze = Raw Layer (no transformation)
- Used as source for Silver layer processing
- Ensures data traceability and reprocessing capability
===========================================================
*/


IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
)


IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50) ,
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
)


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);


IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50)
);
