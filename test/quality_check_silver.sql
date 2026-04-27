/*
===========================================================
Silver Layer - Data Quality Checks (All Tables)
===========================================================

Purpose:
- Validate data quality of all Silver layer tables
- Ensure data is clean, consistent, and ready for analytics
- This script is used ONLY for validation (no data modification)

Coverage:
1. CRM Tables
   - crm_cust_info
   - crm_prd_info
   - crm_sales_details

2. ERP Tables
   - erp_cust_az12
   - erp_loc_a101
   - erp_px_cat_g1v2

Validation Types:
- Duplicate and NULL primary keys
- Unwanted spaces
- Data consistency and standardization
- Business rule validation
- Date validation
- NULL and invalid values

===========================================================
*/

-----------------------------------------------------------
-- 1. crm_cust_info
-----------------------------------------------------------

-- Check total vs distinct (detect duplicates)
SELECT COUNT(cst_id) AS total_count,
       COUNT(DISTINCT cst_id) AS distinct_count
FROM silver.crm_cust_info;

-- Find duplicate or NULL primary keys
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check unwanted spaces in names
SELECT cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
   OR cst_lastname != TRIM(cst_lastname);

-- Check standardized values
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;


-----------------------------------------------------------
-- 2. crm_prd_info
-----------------------------------------------------------

-- Check duplicates
SELECT COUNT(prd_id) AS total_count,
       COUNT(DISTINCT prd_id) AS distinct_count
FROM silver.crm_prd_info;

SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check unwanted spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check invalid cost values
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check standardized values
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Check date validity (start should not be after end)
SELECT *
FROM silver.crm_prd_info
WHERE prd_start_dt > prd_end_dt;


-----------------------------------------------------------
-- 3. crm_sales_details
-----------------------------------------------------------

-- Check NULL critical fields
SELECT *
FROM silver.crm_sales_details
WHERE sls_ord_num IS NULL
   OR sls_prd_key IS NULL
   OR sls_cust_id IS NULL;

-- Check date validity
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;

-- Check business rule: sales = quantity * price
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price;

-- Check negative or zero values
SELECT *
FROM silver.crm_sales_details
WHERE sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0;


-----------------------------------------------------------
-- 4. erp_cust_az12
-----------------------------------------------------------

-- Check invalid birth dates
SELECT *
FROM silver.erp_cust_az12
WHERE bdate < '1919-01-01'
   OR bdate > GETDATE();

-- Check standardized gender values
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- Check NULL customer IDs
SELECT *
FROM silver.erp_cust_az12
WHERE cid IS NULL;


-----------------------------------------------------------
-- 5. erp_loc_a101
-----------------------------------------------------------

-- Check NULL customer IDs
SELECT *
FROM silver.erp_loc_a101
WHERE cid IS NULL;

-- Check standardized country values
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

-- Check unwanted spaces
SELECT *
FROM silver.erp_loc_a101
WHERE cntry != TRIM(cntry);


-----------------------------------------------------------
-- 6. erp_px_cat_g1v2
-----------------------------------------------------------

-- Check NULL IDs
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;

-- Check unwanted spaces
SELECT *
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat)
   OR subcat != TRIM(subcat);

-- Check distinct values for consistency
SELECT DISTINCT cat FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM silver.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM silver.erp_px_cat_g1v2;


-----------------------------------------------------------
-- End of Data Quality Checks
-----------------------------------------------------------
-- If all queries return expected/clean results:
-- ✔ Data is ready for Gold Layer / Reporting
-----------------------------------------------------------
