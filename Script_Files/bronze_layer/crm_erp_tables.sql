/*
====================================================================================================================
				   DDL SCRIPT : CREATE BRONZE CRM AND ERP TABLES
====================================================================================================================
Purpose:
		This script is used to create and manage tables within the 'BRONZE' schema. 
		It ensures that any existing tables are dropped before re-creation, allowing for a clean and 
		consistent structure across the data ingestion layer.
Functionality:
		Checks for the existence of each table in the 'BRONZE' schema and drops it if present.
		Recreates the required tables using the latest DDL (Data Definition Language).
		Ensures the raw data layer is correctly structured and ready for ingestion from source systems.
Usage:
		Run this script to reinitialize the Bronze Layer tables, particularly during:
			* Initial setup of the data pipeline
			* Schema refreshes or updates
			* Environment resets

This helps maintain integrity and alignment with the upstream source data format.
====================================================================================================================
*/

USE [LedgerLink_DWH];

-- CRM TABLES

-- CRM Customer_Info TABLE

IF OBJECT_ID('[Bronze_Layer].crm_customer_info', 'U') IS NOT NULL
	DROP TABLE [Bronze_Layer].crm_customer_info;
CREATE TABLE [Bronze_Layer].crm_customer_info(
	CUST_ID INT,
	CUST_KEY NVARCHAR(50),
	CUST_FIRST_NAME NVARCHAR(50),
	CUST_LAST_NAME NVARCHAR(50),
	CUST_MATERIAL_STATUS NVARCHAR(50),
	CUST_GENDER NVARCHAR(50),
	CUST_CREATE_DATE DATE
);

-- CRM Product_Info TABLE

IF OBJECT_ID('[Bronze_Layer].crm_product_info', 'U') IS NOT NULL
	DROP TABLE [Bronze_Layer].crm_product_info;
CREATE TABLE [Bronze_Layer].crm_product_info(
	PROD_ID INT,
	PROD_KEY NVARCHAR(50),
	PROD_NAME NVARCHAR(50),
	PROD_COST INT,
	PROD_LINE NVARCHAR(50),
	PROD_START_DATE DATETIME,
	PROD_END_DATE DATETIME
);

-- CRM Sales_Details TABLE

IF OBJECT_ID('[Bronze_Layer].crm_sales_details', 'U') IS NOT NULL
	DROP TABLE [Bronze_Layer].crm_sales_details;
DROP TABLE IF EXISTS [Bronze_Layer].crm_sales_details;
CREATE TABLE [Bronze_Layer].crm_sales_details(
	SALES_ORDER_NUM NVARCHAR(50),
	SALES_PROD_KEY NVARCHAR(50),
	SALES_CUST_ID INT,
	SALES_ORDER_DATE DATE,
	SALES_SHIP_DATE DATE,
	SALES_DUE_DATE DATE,
	SALES_SALES INT,
	SALES_QUANTITY INT,
	SALES_PRICE INT
);

-- ERP TABLES

-- ERP Loc_A101 TABLE

IF OBJECT_ID('[Bronze_Layer].erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE [Bronze_Layer].erp_loc_a101;
CREATE TABLE [Bronze_Layer].erp_loc_a101(
	CID NVARCHAR(50),
	CNTRY NVARCHAR(50)
);

-- ERP Cust_AZ12 Table
IF OBJECT_ID('[Bronze_Layer].erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE [Bronze_Layer].erp_cust_az12;
CREATE TABLE [Bronze_Layer].erp_cust_az12(
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50)
);

-- ERP PX_CAT_G1V2 Table
IF OBJECT_ID('[Bronze_Layer].erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE [Bronze_Layer].erp_px_cat_g1v2;
CREATE TABLE [Bronze_Layer].erp_px_cat_g1v2(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50)
);
