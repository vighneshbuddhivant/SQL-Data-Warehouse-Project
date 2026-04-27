/*
===========================================================
Silver Layer Transformation: erp_px_cat_g1v2
===========================================================

Purpose:
- Validate and load product category data from Bronze to Silver layer
- Ensure data quality and consistency before loading

Steps Performed:

1. Data Exploration
- View raw data from bronze.erp_px_cat_g1v2

2. Column Validation
- id column:
    - Verified as clean and consistent
    - No transformation required

3. Data Cleaning (Unwanted Spaces)
- Checked for leading/trailing spaces in:
    - cat (category)
    - subcat (subcategory)
- Result: No unwanted spaces found

4. Data Standardization Check
- Verified distinct values for:
    - cat
    - subcat
    - maintenance
- Result: Data already standardized and consistent

5. Decision
- Since data is clean and consistent:
    - No transformation required
    - Direct load into Silver layer

6. Load into Silver Layer
- Insert data as-is from Bronze to Silver

7. Post-Load Validation
- Verify data successfully loaded into Silver table

===========================================================
*/

-- Step 1: View raw data
SELECT * FROM bronze.erp_px_cat_g1v2;

-- Step 2: Check unwanted spaces in category
SELECT cat 
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat);

-- Step 3: Check unwanted spaces in subcategory
SELECT subcat 
FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat);

-- Step 4: Validate distinct values (data consistency)
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

-- Step 5: Load into Silver layer (no transformation needed)
INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
SELECT id, cat, subcat, maintenance 
FROM bronze.erp_px_cat_g1v2;

-- Step 6: Final verification
SELECT * FROM silver.erp_px_cat_g1v2;
