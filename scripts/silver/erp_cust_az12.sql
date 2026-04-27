/*
===========================================================
Silver Layer Transformation: erp_cust_az12
===========================================================

Purpose:
- Clean and transform ERP customer data from Bronze layer
- Standardize customer ID, birth date, and gender
- Load high-quality data into Silver layer

Steps Performed:

1. Data Exploration
- View raw data from bronze.erp_cust_az12

2. Customer ID Standardization
- Column cid contains prefix 'NAS'
- Remove prefix to align with crm_cust_info (cst_id)
- Ensures proper join between ERP and CRM systems

3. Birth Date Validation
- Check for invalid or out-of-range dates
- Conditions:
    - Date should not be in future
    - Date should be within valid historical range
- Invalid dates converted to NULL

4. Gender Standardization
- Normalize gender values:
    M / MALE → Male
    F / FEMALE → Female
    Others / NULL → N/A
- Ensures consistency across datasets

5. Data Transformation
- Apply all cleaning logic using CASE statements

6. Load into Silver Layer
- Insert cleaned and standardized data into silver.erp_cust_az12

7. Post-Load Validation
- Ensure no invalid dates remain
- Ensure gender values are standardized

===========================================================
*/

-- Step 1: View raw data
SELECT * FROM bronze.erp_cust_az12;

-- Step 2: Transform customer ID
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid 
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12;

-- Step 3: Validate birth date
SELECT bdate 
FROM bronze.erp_cust_az12
WHERE bdate < '1919-01-01' OR bdate > GETDATE();

-- Step 4: Handle invalid birth dates
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid 
END AS cid,
CASE 
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
gen
FROM bronze.erp_cust_az12;

-- Step 5: Check gender values
SELECT DISTINCT gen 
FROM bronze.erp_cust_az12;

-- Step 6: Apply full transformation
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid 
END AS cid,
CASE 
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END AS bdate,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	ELSE 'N/A'
END AS gen
FROM bronze.erp_cust_az12;

-- Step 7: Load into Silver layer
INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
SELECT 
CASE 
	WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	ELSE cid 
END,
CASE 
	WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END,
CASE 
	WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	ELSE 'N/A'
END
FROM bronze.erp_cust_az12;

-- Step 8: Post-load validation

-- Check invalid dates
SELECT bdate 
FROM silver.erp_cust_az12
WHERE bdate < '1919-01-01' OR bdate > GETDATE();

-- Check standardized gender values
SELECT DISTINCT gen 
FROM silver.erp_cust_az12;

-- Final data view
SELECT * FROM silver.erp_cust_az12;
