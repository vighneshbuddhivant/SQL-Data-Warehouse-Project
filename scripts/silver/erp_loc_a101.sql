/*
===========================================================
Silver Layer Transformation: erp_loc_a101
===========================================================

Purpose:
- Clean and standardize ERP location data from Bronze layer
- Prepare customer location data for consistent usage and joins
- Load processed data into Silver layer

Steps Performed:

1. Data Exploration
- View raw data from bronze.erp_loc_a101

2. Customer ID Cleaning
- Column cid contains special characters like '-'
- Remove '-' using REPLACE()
- Align format with crm_cust_info (cst_key) for proper joins

3. Country Standardization
- Column cntry is low cardinality → apply standardization
- Convert codes to meaningful values:
    DE → Germany
    US / USA → United States
    NULL / empty → N/A
    Others → trimmed as-is

4. Data Cleaning
- Remove unwanted spaces using TRIM()
- Handle NULL and blank values

5. Data Transformation
- Apply all cleaning and standardization logic

6. Load into Silver Layer
- Insert cleaned data into silver.erp_loc_a101

7. Post-Load Validation
- Verify standardized country values
- Ensure no inconsistent or null data

===========================================================
*/

-- Step 1: View raw data
SELECT * FROM bronze.erp_loc_a101;

-- Step 2: Clean customer ID
SELECT 
REPLACE(cid, '-', '') AS cid,
cntry
FROM bronze.erp_loc_a101;

-- Step 3: Check distinct country values
SELECT DISTINCT cntry 
FROM bronze.erp_loc_a101;

-- Step 4: Apply full transformation
SELECT 
REPLACE(cid, '-', '') AS cid,
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-- Step 5: Load into Silver layer
INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT 
REPLACE(cid, '-', ''),
CASE 
	WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END
FROM bronze.erp_loc_a101;

-- Step 6: Post-load validation

-- Check standardized country values
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101;

-- Final data view
SELECT * FROM silver.erp_loc_a101;
