/* 
---------------------------------------------------------------------------------------------------------------
Stored Procedure: Load Bronze Layer(Source->Bronze)
---------------------------------------------------------------------------------------------------------------
Script Purpose: 
      This script loads data into bronze schema from external csv files
      It performs the following actions:
      Truncates the bronze tables before loading data.
      Uses the 'bulk insert' command to load data from csv files to bronze tables.

Parameters: None
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze.load_bronze;
-------------------------------------------------------------------------------------------------------------
*/


-- In Bronze layer, we do truncate and insert. Truncate: quickly deletes all rows from table, resetting it to an empty state
-- This is a frequently used SQL code, hence we use stored procedure.
-- WARNING: For PRINT statements: use single quotes only.
-- SQL runs the TRY block and if it fails then it runs the CATCH block to handle the error
-- Track ETL Duration

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '------------------------------------';
		PRINT 'Loading Bronze Layer';
		PRINT '-------------------------------------';

		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '---------------------------------------';

		-- Table : bronze.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data in: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\anushka\Desktop\sql projects\data warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------';
		-- This entire process of truncating and inserting is called as 'Full Load'.
		-- Quality Check: Check that the data is in the correct column and has not shifted
		-- SELECT * FROM bronze.crm_cust_info;
		-- SELECT COUNT(*) FROM bronze.crm_cust_info;


		-- Table: bronze.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>>Inserting data in bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\anushka\Desktop\sql projects\data warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '-------------------------';
		-- SELECT * FROM bronze.crm_prd_info;
		-- SELECT COUNT(*) FROM bronze.crm_prd_info;


		--Table: bronze.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data in: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\anushka\Desktop\sql projects\data warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------------';
		-- SELECT * FROM bronze.crm_sales_details;
		-- SELECT COUNT(*) FROM bronze.crm_sales_details;

		PRINT '---------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '---------------------------------------------';

		--Table: bronze.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data in: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\anushka\Desktop\sql projects\data warehouse\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '----------------------';
		-- SELECT * FROM bronze.erp_loc_a101;
		-- SELECT COUNT(*) FROM bronze.erp_loc_a101;


		-- Table: bronze.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data in: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\anushka\Desktop\sql projects\data warehouse\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
        PRINT '---------------------';
		-- SELECT * FROM bronze.erp_cust_az12;
		-- SELECT COUNT(*) FROM bronze.erp_cust_az12;


		-- Table: bronze.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Inserting Data in: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\anushka\Desktop\sql projects\data warehouse\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load Duration: '+ CAST (DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
		PRINT '---------------------';
		-- SELECT * FROM bronze.erp_px_cat_g1v2;
		-- SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

		SET @batch_end_time = GETDATE();
		PRINT '----------------------------------------------';
		PRINT 'Loading Bronze Layer is completed';
		PRINT 'Total Load Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '------------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '================================================';
		PRINT 'Error occured during loading bronze layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT '================================================';
	END CATCH
END
