/*
===========================================================
Silver Layer Transformation: crm_prd_info
===========================================================

Purpose:
- Clean and transform raw product data from Bronze layer
- Standardize values and handle data quality issues
- Load processed data into Silver layer for further use

Steps Performed:

1. Data Exploration
- View raw data from bronze.crm_prd_info

2. Primary Key Validation
- Check for duplicates and NULL values in prd_id
- Ensure data integrity before processing

3. Data Cleaning
- Check for unwanted spaces in prd_nm
- Validate prd_cost for NULL or negative values
- Replace NULL prd_cost with 0

4. Data Standardization
- Convert prd_line codes into meaningful values:
    M → Mountain
    R → Road
    S → Other Sales
    T → Touring
    NULL/others → N/A

5. Data Transformation
- Split prd_key into:
    cat_key → category key (first part)
    prd_key → actual product key (second part)
- Replace '-' with '_' in category key

6. Date Handling (SCD Type Logic)
- prd_start_dt → cast to DATE
- prd_end_dt → calculated using LEAD()
  (next start date - 1 day)
- Helps in tracking product validity periods

7. Load into Silver Layer
- Insert cleaned and transformed data into silver.crm_prd_info

8. Post-Load Validation
- Ensure no duplicates
- Ensure no NULL primary keys
- Validate cleaned data

===========================================================
*/

-- Step 1: View raw data
SELECT * FROM bronze.crm_prd_info;

-- Step 2: Check duplicates and NULLs in primary key
SELECT COUNT(prd_id) AS Total_Count, 
       COUNT(DISTINCT prd_id) AS Total_distinct_count
FROM bronze.crm_prd_info;

SELECT prd_id, COUNT(*) 
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Step 3: Check unwanted spaces
SELECT prd_nm 
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Step 4: Check invalid cost values
SELECT prd_cost 
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Step 5: Check data consistency
SELECT DISTINCT prd_line 
FROM bronze.crm_prd_info;

-- Step 6: Validate date logic
SELECT * 
FROM bronze.crm_prd_info
WHERE prd_start_dt < prd_end_dt;

-- Step 7: Transform data
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_key,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'N/A'
	END AS prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt,
	DATEADD(DAY, -1, 
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)
	) AS prd_end_dt
FROM bronze.crm_prd_info;

-- Step 8: Load into Silver layer
INSERT INTO silver.crm_prd_info(
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
	prd_id,
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
	prd_nm,
	ISNULL(prd_cost,0),
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'N/A'
	END,
	CAST(prd_start_dt AS DATE),
	DATEADD(DAY, -1, 
		LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)
	)
FROM bronze.crm_prd_info;

-- Step 9: Post-load validation

-- Check counts
SELECT COUNT(prd_id) AS Total_Count, 
       COUNT(DISTINCT prd_id) AS Total_distinct_count
FROM silver.crm_prd_info;

-- Check duplicates
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

-- Validate standardized values
SELECT DISTINCT prd_line 
FROM silver.crm_prd_info;

-- Validate date logic
SELECT * 
FROM silver.crm_prd_info
WHERE prd_start_dt < prd_end_dt;

-- Final data view
SELECT * FROM silver.crm_prd_info;
