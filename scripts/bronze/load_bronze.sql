/* It is suggested to add prints to track execution, debug issues and understand the flow*/
/* save the frequently used SQL code in stored procedures in database*/

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

EXEC bronze.load_bronze;


CREATE or ALTER PROCEDURE bronze.load_bronze /*(it is the naming convention)*/ AS
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME;  /*@ tells SQL Server “this is a variable”*/
	DECLARE @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		PRINT '**************************************************';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '**************************************************';

		PRINT '----------------------------------------------------';
		PRINT 'LOADING CRM TABLES';
		PRINT '----------------------------------------------------';
		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '**** TRUNCATING TABLE: bronze.crm_cust_info ****';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '**** INSERTING DATA INTO TABLE: bronze.crm_cust_info ****';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Load Duration : '+ CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
	
		SET @start_time = GETDATE();
		PRINT '**** TRUNCATING TABLE:bronze.crm_prd_info ****';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '**** INSERTING DATA INTO TABLE:bronze.crm_prd_info ****';
		BULK INSERT bronze.crm_prd_info
		from 'E:\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Load Duration : '+ CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '**** TRUNCATING TABLE: bronze.crm_sales_details ****';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '**** INSERTING DATA INTO TABLE: bronze.crm_sales_details ****';
		BULK INSERT bronze.crm_sales_details
		from 'E:\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Load Duration : '+ CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';


		PRINT '----------------------------------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '**** TRUNCATING TABLE: bronze.erp_cust_az12****';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '**** INSERTING DATA INTO TABLE: bronze.erp_cust_az12****';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Load Duration : '+ CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '**** TRUNCATING TABLE: bronze.erp_loc_a101 ****';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '**** INSERTING DATA INTO TABLE : bronze.erp_loc_a101 ****';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @end_time = GETDATE();

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Load Duration : '+ CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @start_time = GETDATE();
		PRINT '**** TRUNCATING TABLE: bronze.erp_px_cat_g1v2 ****';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '**** INSERTING DATA INTO TABLE: bronze.erp_px_cat_g1v2 ****';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\SQL\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
			);
		SET @batch_end_time = GETDATE();
		SET @end_time = GETDATE();

		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';
		PRINT 'Load Duration : '+ CAST (DATEDIFF(second, @start_time,@end_time) AS NVARCHAR)+'seconds';
		PRINT'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		PRINT '++++++++++++++++++++++++++++++++++';
		PRINT 'Bronze Layer Load Duration : '+ CAST (DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR)+'seconds';
		PRINT '++++++++++++++++++++++++++++++++++';


		END TRY	
		BEGIN CATCH
			PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
			PRINT 'ERROR OCCURED IN LOADING THE BRONZE LAYER';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT'!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
		END CATCH
	END


	-- NOTE:
-- DATEDIFF(second, start, end) counts only FULL seconds.
-- Example:
--   Actual Time   -> Output
--   0.95 seconds  -> 0 seconds
--   1.01 seconds  -> 1 second
-- So fast loads may display 0 seconds even though they ran successfully.
