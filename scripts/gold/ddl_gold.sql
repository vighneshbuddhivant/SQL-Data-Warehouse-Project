/*
===========================================================
Gold Layer - Data Model (Dimensions & Fact Views)
===========================================================

Purpose:
- Create analytical layer (Gold) for reporting and BI usage
- Provide business-ready, clean, and structured data
- Implement Star Schema:
    - Dimension Tables → descriptive attributes
    - Fact Table → transactional data

Objects Created:
1. gold.dim_customers   → Customer Dimension
2. gold.dim_products    → Product Dimension
3. gold.fact_sales      → Sales Fact

Key Concepts Used:
- Surrogate Keys using ROW_NUMBER()
- LEFT JOINs to combine multiple sources
- Data enrichment from CRM + ERP
- Filtering only active records (SCD handling)
- Business-friendly column naming

===========================================================
*/


-- =============================================================================
-- 1. Dimension: Customers
-- =============================================================================
/*
Purpose:
- Create unified customer dimension
- Combine CRM (primary source) + ERP (additional details)

Logic:
- CRM is primary source for customer data
- ERP used for:
    - Birthdate
    - Gender fallback
    - Country
- Gender priority:
    1. CRM data
    2. ERP fallback
    3. Default → 'n/a'

Surrogate Key:
- customer_key generated using ROW_NUMBER()

Join Logic:
- crm_cust_info ↔ erp_cust_az12 (cid)
- crm_cust_info ↔ erp_loc_a101 (cid)
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr   -- CRM प्राथमिक source
        ELSE COALESCE(ca.gen, 'n/a')                 -- ERP fallback
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO



-- =============================================================================
-- 2. Dimension: Products
-- =============================================================================
/*
Purpose:
- Create product dimension with category details

Logic:
- Product data from CRM
- Category data from ERP
- Join using category_id

Filtering:
- Only current/active records included
- Historical records filtered using:
    prd_end_dt IS NULL

Surrogate Key:
- product_key generated using ROW_NUMBER()

Join Logic:
- crm_prd_info ↔ erp_px_cat_g1v2
*/

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Only active records
GO



-- =============================================================================
-- 3. Fact Table: Sales
-- =============================================================================
/*
Purpose:
- Create fact table for sales transactions
- Link transactions with customer and product dimensions

Logic:
- Base table: crm_sales_details
- Enrich with:
    - product_key from dim_products
    - customer_key from dim_customers

Grain:
- One row per sales transaction (order level)

Join Logic:
- crm_sales_details ↔ dim_products (product_number)
- crm_sales_details ↔ dim_customers (customer_id)

Measures:
- sales_amount
- quantity
- price

Dates:
- order_date
- shipping_date
- due_date
*/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO


/***********************************************************
Final Note:
- Gold Layer is ready for:
    ✔ Power BI / Tableau
    ✔ Reporting
    ✔ Business Analysis

- Star Schema Achieved:
    fact_sales → central table
    dim_customers, dim_products → dimensions

===========================================================
*/
