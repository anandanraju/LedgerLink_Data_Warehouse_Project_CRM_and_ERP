/*
===================================================================================================================================================
					      DDL SCRIPT: CREATE STORED PROCEDURE FOR SILVER LAYER
===================================================================================================================================================
Purpose:
		This stored procedure is built to remove all existing data from the Silver Layer tables in the LedgerLink Data Warehouse (DWH). 
		It uses the TRUNCATE statement to efficiently clear transformed data for both CRM and ERP domains, while preserving the table structures.

Functionality:
		Executes a TRUNCATE operation on all targeted tables within the Silver_Layer schema.
		Applies to both CRM and ERP processed data tables.
		Offers faster execution and lower transaction log usage compared to DELETE.
		Prepares the Silver Layer for a clean data reload from the Bronze Layer or staging environment.
===================================================================================================================================================
*/

-- Run this after Updating the Silver Layer
-- EXEC [Silver_Layer].Load_Silver;

-- SILVER LAYER ETL PROCESS

CREATE OR ALTER PROCEDURE [Silver_Layer].Load_Silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	BEGIN TRY
		SET @batch_start = GETDATE();
		PRINT '==============================================';
		PRINT 'Data Loading Initiated for Silver Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();		
		
		-- SILVER - CRM CUSTOMER INFO TABLE LOAD

		-- RUN ONLY WHEN LOADED WRONG DATA
		PRINT '>> Truncating Table: Silver.CRM_Cust_Info'
		TRUNCATE TABLE [Silver_Layer].[crm_customer_info];

		-- INSERT INTO CRM_CUST_INFO TABLE
		PRINT '>> Inserting Date Into: Silver.CRM_Cust_Info'
		INSERT INTO [Silver_Layer].[crm_customer_info] (
				CUST_ID,
				CUST_KEY, 
				CUST_FIRST_NAME,
				CUST_LAST_NAME,
				CUST_MATERIAL_STATUS,
				CUST_GENDER,
				CUST_CREATE_DATE
				)
		SELECT 
			CUST_ID,
			CUST_KEY,
			TRIM(CUST_FIRST_NAME) AS CUST_FIRST_NAME,
			TRIM(CUST_LAST_NAME) AS CUST_LAST_NAME,
			CASE UPPER(TRIM(CUST_MATERIAL_STATUS))
				WHEN 'S' THEN 'SINGLE'
				WHEN 'M' THEN 'MARRIED'
				ELSE 'N/A'
			END CUST_MATERIAL_STATUS,
			CASE UPPER(TRIM(CUST_GENDER))
				WHEN 'M' THEN 'MALE'
				WHEN 'F' THEN 'FEMALE'
				ELSE 'N/A'
			END CUST_GENDER,
			CUST_CREATE_DATE
		FROM
		(SELECT *,
		ROW_NUMBER() OVER(PARTITION BY CUST_ID ORDER BY CUST_CREATE_DATE DESC) AS FLAT_RANK 
		FROM [Bronze_Layer].[crm_customer_info]
		WHERE CUST_ID IS NOT NULL) T
		WHERE FLAT_RANK=1;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';
		
		SET @start_time = GETDATE();
 
		-- SILVER - CRM PRODUCT INFO TABLE LOAD

		-- RUN ONLY WHEN LOADED WRONG DATA
		PRINT '>> Truncating Table: Silver.CRM_Product_Info'
		TRUNCATE TABLE [Silver_Layer].[crm_product_info];

		-- INSERT INTO CRM_PRODUCT_INFO TABLE
		PRINT '>> Inserting Date Into: Silver.CRM_Product_Info'
		INSERT INTO [Silver_Layer].[crm_product_info](
			PROD_ID,	
			CAT_ID,	
			PROD_KEY,	
			PROD_NAME,	
			PROD_COST,	
			PROD_LINE,	
			PROD_START_DATE,	
			PROD_END_DATE
		)
		SELECT
			PROD_ID,
			REPLACE(SUBSTRING(PROD_KEY,1,5),'-','_') AS CAT_ID,
			SUBSTRING(PROD_KEY,7,LEN(PROD_KEY)) AS PROD_KEY,
			PROD_NAME,
			ISNULL(PROD_COST,0) AS PROD_COST,
			CASE UPPER(TRIM(PROD_LINE))
				 WHEN 'M' THEN 'Mountain'
				 WHEN 'R' THEN 'Road'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'N/A'
			END PROD_LINE,
			CAST(PROD_START_DATE AS DATE) AS PROD_START_DATE,
			CAST(LEAD(PROD_START_DATE) OVER(PARTITION BY PROD_KEY ORDER BY PROD_START_DATE)-1 AS DATE) AS PROD_END_DATE
		FROM [Bronze_Layer].[crm_product_info]

		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();

		-- SILVER - CRM SALES DETAILS TABLE LOAD

		-- RUN ONLY WHEN LOADED WRONG DATA
		PRINT '>> Truncating Table: Silver.CRM_Sales_Details'
		TRUNCATE TABLE [Silver_Layer].[crm_sales_details];

		-- INSERT INTO CRM_SALES_DETAILS TABLE
		PRINT '>> Inserting Date Into: Silver.CRM_Sales_Details'
		INSERT INTO [Silver_Layer].[crm_sales_details](
			SALES_ORDER_NUM,
			SALES_PROD_KEY,
			SALES_CUST_ID,
			SALES_ORDER_DATE,
			SALES_SHIP_DATE,
			SALES_DUE_DATE,
			SALES_SALES,
			SALES_QUANTITY,
			SALES_PRICE
		)
		SELECT 
			SALES_ORDER_NUM,
			SALES_PROD_KEY,
			SALES_CUST_ID,
			CASE WHEN SALES_ORDER_DATE =0 OR LEN(SALES_ORDER_DATE) != 8 THEN NULL
				 ELSE CAST(CAST(SALES_ORDER_DATE AS VARCHAR) AS DATE)
				 END SALES_ORDER_DATE,
			CASE WHEN SALES_SHIP_DATE =0 OR LEN(SALES_SHIP_DATE) != 8 THEN NULL
				 ELSE CAST(CAST(SALES_SHIP_DATE AS VARCHAR) AS DATE)
				 END SALES_SHIP_DATE,
			CASE WHEN SALES_DUE_DATE =0 OR LEN(SALES_DUE_DATE) != 8 THEN NULL
				 ELSE CAST(CAST(SALES_DUE_DATE AS VARCHAR) AS DATE)
				 END SALES_DUE_DATE,
			CASE WHEN SALES_SALES IS NULL OR SALES_SALES <= 0 OR SALES_SALES != SALES_QUANTITY*ABS(SALES_PRICE)
				 THEN SALES_QUANTITY*ABS(SALES_PRICE)
				 ELSE SALES_SALES
				 END SALES_SALES,
			SALES_QUANTITY,
			CASE WHEN SALES_PRICE IS NULL OR SALES_PRICE <= 0
				 THEN SALES_SALES/NULLIF(SALES_QUANTITY,0)
				 ELSE SALES_PRICE
				 END SALES_PRICE
		FROM [Bronze_Layer].[crm_sales_details];

		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();

		-- SILVER - ERP CUSTOMER AZ12 TABLE LOAD

		-- RUN ONLY WHEN LOADED WRONG DATA
		PRINT '>> Truncating Table: Silver.ERP_CUST_AZ12'
		TRUNCATE TABLE [Silver_Layer].[erp_cust_az12];

		-- INSERT INTO CRM_CUST_INFO TABLE
		PRINT '>> Inserting Date Into: Silver.ERP_CUST_AZ12'
		INSERT INTO [Silver_Layer].[erp_cust_az12](
			CID,
			BDATE,
			GEN
		)
		SELECT 
			CASE WHEN CID LIKE '%NAS%' THEN SUBSTRING(CID, 4,LEN(CID))
				 ELSE CID
			END CID,
			CASE WHEN BDATE > GETDATE() THEN NULL
				 ELSE BDATE
			END BDATE,
			CASE 
				WHEN  UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
				WHEN  UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
				ELSE 'N/A'
			END GEN
		FROM [Bronze_Layer].[erp_cust_az12];

		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();

		-- SILVER - ERP CUSTOMER A101 TABLE LOAD

		-- RUN ONLY WHEN LOADED WRONG DATA
		PRINT '>> Truncating Table: Silver.ERP_LOC_A101'
		TRUNCATE TABLE [Silver_Layer].[erp_loc_a101];

		-- INSERT INTO CRM_CUST_INFO TABLE
		PRINT '>> Inserting Date Into: Silver.ERP_LOC_A101'
		INSERT INTO [Silver_Layer].[erp_loc_a101] (
			CID,
			CID_KEY,
			COUNTRY
		) 
		SELECT 	
			REPLACE(CID,'-','') AS CID,
			CASE WHEN CID LIKE '%AW%' THEN SUBSTRING(CID,7,LEN(CID))
				 ELSE CID
			END CID_KEY,
			CASE WHEN UPPER(TRIM(COUNTRY)) IN ('US', 'UNITED STATES','USA') THEN 'United States'
				 WHEN UPPER(TRIM(COUNTRY)) IN ('DE', 'GERMANY') THEN 'Germany'
				 WHEN TRIM(COUNTRY) = '' OR COUNTRY IS NULL THEN 'N/A'
			ELSE TRIM(COUNTRY)
			END COUNTRY
		FROM [Bronze_Layer].[erp_loc_a101];

		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @start_time = GETDATE();

		-- SILVER - ERP CUSTOMER PXCATG1V2 TABLE LOAD

		-- RUN ONLY WHEN LOADED WRONG DATA
		PRINT '>> Truncating Table: Silver.ERP_PX_CAT_G1V2'
		TRUNCATE TABLE [Silver_Layer].[erp_px_cat_g1v2];

		-- INSERT INTO CRM_CUST_INFO TABLE
		PRINT '>> Inserting Date Into: Silver.ERP_PX_CAT_G1V2'
		INSERT INTO [Silver_Layer].[erp_px_cat_g1v2](
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		SELECT
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM [Bronze_Layer].[erp_px_cat_g1v2];
		
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '----------------------------------------------';

		SET @batch_end = GETDATE();
		PRINT '----------------------------------------------'
		PRINT 'Silver Layer Loading Completed Successfully ';
		PRINT 'Total Load Duration: '+ CAST(DATEDIFF(SECOND,@batch_start,@batch_end) AS NVARCHAR) +' seconds';
		PRINT '----------------------------------------------'
	END TRY
	BEGIN CATCH
		PRINT '----------------------------------------------'
		PRINT 'ERROR OCCURED WHILE LOADING SILVER LAYER'
		PRINT '----------------------------------------------'
		PRINT 'Error Message:'+ ERROR_MESSAGE();
		PRINT 'Error Number:'+ CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State:'+ CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '----------------------------------------------'
	END CATCH
END
