EXEC [Bronze_Layer].load_bronze;

CREATE OR ALTER PROCEDURE [Bronze_Layer].Load_Bronze AS

BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
		SET @batch_start = GETDATE();
		PRINT '==============================================';
		PRINT 'Data Loading Initiated for Bronze Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: [Bronze_Layer].crm_customer_info'
		TRUNCATE TABLE [Bronze_Layer].crm_customer_info;
		PRINT '>>> Inserting Data Into: [Bronze_Layer].crm_customer_info'
		BULK INSERT [Bronze_Layer].crm_customer_info
		FROM 'D:\Data Science\SQL_Project\Ledgerlink Datawarehouse Project\GIt_Folders\Datasets\crm_source_datasets\customer_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: [Bronze_Layer].crm_product_info'
		TRUNCATE TABLE [Bronze_Layer].crm_product_info;
		PRINT '>>> Inserting Data Into: [Bronze_Layer].crm_product_info'
		BULK INSERT [Bronze_Layer].crm_product_info
		FROM 'D:\Data Science\SQL_Project\Ledgerlink Datawarehouse Project\GIt_Folders\Datasets\crm_source_datasets\product_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: [Bronze_Layer].crm_sales_details'
		TRUNCATE TABLE [Bronze_Layer].crm_sales_details;
		PRINT '>>> Inserting Data Into: [Bronze_Layer].crm_sales_details'
		BULK INSERT [Bronze_Layer].crm_sales_details
		FROM 'D:\Data Science\SQL_Project\Ledgerlink Datawarehouse Project\GIt_Folders\Datasets\crm_source_datasets\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: [Bronze_Layer].erp_loc_a101'
		TRUNCATE TABLE [Bronze_Layer].erp_loc_a101;
		PRINT '>>> Inserting Data Into: [Bronze_Layer].erp_loc_a101'
		BULK INSERT [Bronze_Layer].erp_loc_a101
		FROM 'D:\Data Science\SQL_Project\Ledgerlink Datawarehouse Project\GIt_Folders\Datasets\erp_source_datasets\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: [Bronze_Layer].erp_cust_az12'
		TRUNCATE TABLE [Bronze_Layer].erp_cust_az12;
		PRINT '>>> Inserting Data Into: [Bronze_Layer].erp_cust_az12'
		BULK INSERT [Bronze_Layer].erp_cust_az12
		FROM 'D:\Data Science\SQL_Project\Ledgerlink Datawarehouse Project\GIt_Folders\Datasets\erp_source_datasets\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: [Bronze_Layer].erp_px_cat_g1v2'
		TRUNCATE TABLE [Bronze_Layer].erp_px_cat_g1v2;
		PRINT '>>> Truncating Table: [Bronze_Layer].erp_px_cat_g1v2'
		BULK INSERT [Bronze_Layer].erp_px_cat_g1v2
		FROM 'D:\Data Science\SQL_Project\Ledgerlink Datawarehouse Project\GIt_Folders\Datasets\erp_source_datasets\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @batch_end = GETDATE();
		PRINT '----------------------------------------------'
		PRINT 'Bronze Layer Loading Completed Successfully ';
		PRINT 'Total Load Duration: '+ CAST(DATEDIFF(SECOND,@batch_start,@batch_end) AS NVARCHAR) +' seconds';
		PRINT '----------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '----------------------------------------------'
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER'
		PRINT '----------------------------------------------'
		PRINT 'Error Message:'+ ERROR_MESSAGE();
		PRINT 'Error Number:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State:'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------'
	END CATCH
END
