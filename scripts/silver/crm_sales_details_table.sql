/*
===========================================================
Silver Layer Transformation: crm_sales_details
===========================================================

Purpose:
- Clean and transform raw sales data from Bronze layer
- Handle data quality issues (dates, nulls, negatives, mismatches)
- Apply business rules and load into Silver layer

Steps Performed:

1. Data Cleaning (Unwanted Spaces)
- Check for leading/trailing spaces in string columns
- Ensure clean order numbers and keys

2. Date Handling (Critical Step)
- Dates are stored as INT in format YYYYMMDD
- Validate:
    - Length should be 8
    - Value should be > 0
- Convert valid integers to DATE
- Invalid values converted to NULL

3. Date Validation
- Ensure logical consistency:
    - Order Date <= Ship Date
    - Order Date <= Due Date

4. Data Quality Checks (Business Rules)
- Validate sales, quantity, price:
    - No NULL values
    - No negative or zero values
    - Ensure: sales = quantity * price

5. Data Correction Logic
- If sales is invalid → derive using quantity * price
- If price is invalid → derive using sales / quantity
- Use ABS() to handle negative price values
- Use NULLIF() to avoid divide-by-zero errors

6. Data Transformation
- Apply all cleaning and business rules in SELECT

7. Load into Silver Layer
- Insert cleaned and transformed data into silver.crm_sales_details

8. Final Verification
- Validate transformed data in Silver layer

===========================================================
*/

-- Step 1: Check unwanted spaces
SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Step 2: Validate date format (INT → DATE)
SELECT sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;

-- Step 3: Convert INT dates to DATE
SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details;

-- Step 4: Validate date relationships
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt 
   OR sls_order_dt > sls_due_dt;

-- Step 5: Identify invalid business data
SELECT
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_sales IS NULL 
   OR sls_quantity IS NULL 
   OR sls_price IS NULL 
   OR sls_sales * sls_quantity != sls_price
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- Step 6: Apply transformation and business rules
SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL 
		  OR sls_sales <= 0 
		  OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  
	END AS sls_price
FROM bronze.crm_sales_details;

-- Step 7: Load into Silver layer
INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
	END,
	CASE 
		WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
	END,
	CASE 
		WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
	END,
	CASE 
		WHEN sls_sales IS NULL 
		  OR sls_sales <= 0 
		  OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END,
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  
	END
FROM bronze.crm_sales_details;

-- Step 8: Final verification
SELECT * FROM silver.crm_sales_details;
