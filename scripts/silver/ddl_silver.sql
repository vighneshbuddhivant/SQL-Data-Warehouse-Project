/*
===========================================================
Silver Layer Table Creation Script (Medallion Architecture)
===========================================================

Purpose:
- Create all Silver layer tables
- Silver layer stores cleaned and transformed data
- Acts as an intermediate layer between Bronze (raw) and Gold (business-ready)

Key Characteristics:
- Data is deduplicated and standardized
- Data quality checks applied (nulls, formats, consistency)
- Business rules partially applied
- Adds metadata column: dwh_create_date for tracking load time

Approach:
- Drop table if exists (to avoid conflicts)
- Recreate tables with same structure as Bronze + additional metadata column

===========================================================
CRM TABLES (Transformed Data from Bronze CRM)
===========================================================

1. silver.crm_cust_info
- Cleaned customer master data
- Deduplicated using business keys
- Stores latest valid customer records

2. silver.crm_prd_info
- Cleaned product data
- Standardized product attributes
- Maintains product lifecycle dates

3. silver.crm_sales_details
- Cleaned transactional data
- Ensures consistency in sales, quantity, and pricing

===========================================================
ERP TABLES (Transformed Data from Bronze ERP)
===========================================================

4. silver.erp_loc_a101
- Cleaned location data
- Standardized country values

5. silver.erp_cust_az12
- Cleaned customer demographic data
- Ensures valid birthdate and gender values

6. silver.erp_px_cat_g1v2
- Cleaned product category mapping
- Standardized category and subcategory values

===========================================================
Metadata Column:
- dwh_create_date:
  Captures record load timestamp
  Helps in auditing, debugging, and incremental processing

===========================================================
Notes:
- Silver layer improves data quality before moving to Gold layer
- No heavy aggregations here (done in Gold)
- Acts as trusted source for downstream processing
===========================================================
*/

IF OBJECT_ID('silver.crm_cust_info','U') IS NOT NULL
	DROP TABLE silver.crm_cust_info
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE ,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)


IF OBJECT_ID('silver.crm_prd_info','U') IS NOT NULL
	DROP TABLE silver.crm_prd_info
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50) ,
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
)


IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_prd_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);


IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintenance  NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
