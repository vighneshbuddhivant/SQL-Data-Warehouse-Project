/*
===========================================================
Stored Procedure: bronze.load_bronze
===========================================================

Purpose:
- Loads raw data into Bronze layer tables using BULK INSERT
- Implements Medallion Architecture (Bronze = Raw Layer)
- Handles full load using TRUNCATE + INSERT approach

Process Flow:
1. Start batch timer
2. Load CRM tables
   - crm_cust_info
   - crm_prd_info
   - crm_sales_details
3. Load ERP tables
   - erp_loc_a101
   - erp_cust_az12
   - erp_px_cat_g1v2
4. Track execution time for each table
5. Track total batch execution time
6. Handle errors using TRY-CATCH

Key Features:
- Uses BULK INSERT for high-performance data loading
- Uses TRUNCATE to remove old data before load (full refresh)
- Logs execution time for monitoring and debugging
- Prints execution steps for visibility

Error Handling:
- Captures error message, number, and state
- Helps in debugging failures during load process

Notes:
- Ensure file paths are accessible to SQL Server
- Bronze layer stores raw data without transformation
- Further processing happens in Silver and Gold layers
===========================================================
*/


EXEC bronze.load_bronze;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS

BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY 
	    SET @batch_start_time = GETDATE();
		PRINT '=======================================';
		PRINT 'Loading Bronze Layer.....'
		PRINT '=======================================';

		PRINT '+++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading CRM Tabels';
		PRINT '+++++++++++++++++++++++++++++++++++++++';


		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info 
		PRINT '>> Insert Into Table: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info 
		FROM 'E:\PERSONAL_FILES\SQL- DATA ENGINEER\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------'
		
		
		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Insert Into Table: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\PERSONAL_FILES\SQL- DATA ENGINEER\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Insert Into Table: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\PERSONAL_FILES\SQL- DATA ENGINEER\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'


		PRINT '+++++++++++++++++++++++++++++++++++++++';
		PRINT 'Loading ERP Tabels';
		PRINT '+++++++++++++++++++++++++++++++++++++++';


		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101' 
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Insert Into Table: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\PERSONAL_FILES\SQL- DATA ENGINEER\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Insert Into Table: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\PERSONAL_FILES\SQL- DATA ENGINEER\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'



		SET @start_time=GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Insert Into Table: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\PERSONAL_FILES\SQL- DATA ENGINEER\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Loading Time :' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-------------------------------------'


		SET @batch_end_time=GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END 
