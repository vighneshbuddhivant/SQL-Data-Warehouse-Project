/*
===========================================================
Stored Procedure: silver.load_silver
===========================================================

Purpose:
- Load cleaned and transformed data from Bronze layer into Silver layer
- Apply all data cleaning, transformation, and standardization logic
- Ensure high-quality, analytics-ready data

Overview:
- This procedure truncates existing Silver tables
- Reloads data from Bronze tables after applying transformations
- Maintains logging using PRINT statements for monitoring
- Tracks execution time for each table and overall batch

Tables Covered:

1. CRM Tables
   - crm_cust_info
   - crm_prd_info
   - crm_sales_details

2. ERP Tables
   - erp_cust_az12
   - erp_loc_a101
   - erp_px_cat_g1v2

Key Transformations Applied:

- Deduplication using ROW_NUMBER()
- Removal of unwanted spaces using TRIM()
- Data standardization using CASE statements
- Handling NULL and invalid values
- Date conversion and validation
- Business rule application (sales = quantity * price)
- String manipulation (SUBSTRING, REPLACE)

Execution Flow:

1. Start batch timer
2. Load CRM tables:
   - Truncate table
   - Insert cleaned data
   - Log execution time

3. Load ERP tables:
   - Truncate table
   - Insert cleaned data
   - Log execution time

4. End batch timer
5. Print total execution duration

Error Handling:

- TRY-CATCH block implemented
- Captures:
   - Error message
   - Error number
   - Error state
- Helps in debugging and monitoring failures

Usage:
- Execute procedure using:
    EXEC silver.load_silver;

Notes:
- Ensure Bronze layer tables are loaded before execution
- File paths and source data should be valid
- Procedure is idempotent due to TRUNCATE + INSERT logic

===========================================================
*/

EXEC silver.laod_silver

CREATE OR ALTER PROCEDURE silver.laod_silver AS 
BEGIN 

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY 
	    SET @batch_start_time = GETDATE();
		PRINT '=======================================';
		PRINT 'Loading silver Layer.....'
		PRINT '=======================================';

		PRINT '+++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading CRM Tabels';
		PRINT '+++++++++++++++++++++++++++++++++++++++';


		SET @start_time=GETDATE();

		PRINT '>> Truncating Table: silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Insert Into Table: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info 
		(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname),
			TRIM(cst_lastname),
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'N/A'
			END,
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'N/A'
			END,
			cst_create_date
		FROM (
			SELECT *, 
				   ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1;
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Insert Into Table: silver.crm_prd_info'
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
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Insert Into Table: silver.crm_sales_details'
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
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		PRINT '+++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading ERP Tabels';
		PRINT '+++++++++++++++++++++++++++++++++++++++';



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT '>> Insert Into Table: silver.erp_cust_az12'
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
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT '>> Insert Into Table: silver.erp_loc_a101'
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
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT '>> Insert Into Table: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT id, cat, subcat, maintenance 
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'


		SET @batch_end_time=GETDATE();
		PRINT '=========================================='
		PRINT 'Loading silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING silver LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
