/*
====================================================================================================================
								               DDL SCRIPT : CREATE SILVER CRM AND ERP TABLES
====================================================================================================================
Purpose:
		This script is designed to manage the creation of tables within the 'SILVER' schema. 
		It first checks for the existence of each target table and drops them if they already exist, 
		ensuring a clean slate before recreating the tables with the updated structure.
Functionality:
		Drops existing tables in the 'SILVER' schema if they are already present.
		Recreates the tables using the latest Data Definition Language (DDL) structure.
		Helps maintain consistency and avoid conflicts during schema updates or migrations.
Usage:
		Run this script whenever you need to redefine or refresh the structure of the Silver Layer tables 
		for example, during schema evolution, testing, or data pipeline setup.
====================================================================================================================
*/

USE [LedgerLink_DWH];

-- CRM TABLES

-- CRM CUSTOMER_INFO TABLE

IF OBJECT_ID('[silver_Layer].crm_customer_info', 'U') IS NOT NULL
	DROP TABLE [silver_Layer].crm_customer_info;
CREATE TABLE [silver_Layer].crm_customer_info(
	CUST_ID INT,
	CUST_KEY NVARCHAR(50),
	CUST_FIRST_NAME NVARCHAR(50),
	CUST_LAST_NAME NVARCHAR(50),
	CUST_MATERIAL_STATUS NVARCHAR(50),
	CUST_GENDER NVARCHAR(50),
	CUST_CREATE_DATE DATE,
	DWH_CREATE_DATE DATETIME2 DEFAULT GETDATE()
);

-- CRM PRODUCT_INFO TABLE

IF OBJECT_ID('[silver_Layer].crm_product_info', 'U') IS NOT NULL
	DROP TABLE [silver_Layer].crm_product_info;
CREATE TABLE [silver_Layer].crm_product_info(
	PROD_ID INT,
	CAT_ID NVARCHAR(50),
	PROD_KEY NVARCHAR(50),
	PROD_NAME NVARCHAR(50),
	PROD_COST INT,
	PROD_LINE NVARCHAR(50),
	PROD_START_DATE DATE,
	PROD_END_DATE DATE,
	DWH_CREATE_DATE DATETIME2 DEFAULT GETDATE()
);

-- CRM SALES_DETAILS TABLE

IF OBJECT_ID('[silver_Layer].crm_sales_details', 'U') IS NOT NULL
	DROP TABLE [silver_Layer].crm_sales_details;
DROP TABLE IF EXISTS [silver_Layer].crm_sales_details;
CREATE TABLE [silver_Layer].crm_sales_details(
	SALES_ORDER_NUM NVARCHAR(50),
	SALES_PROD_KEY NVARCHAR(50),
	SALES_CUST_ID INT,
	SALES_ORDER_DATE DATE,
	SALES_SHIP_DATE DATE,
	SALES_DUE_DATE DATE,
	SALES_SALES INT,
	SALES_QUANTITY INT,
	SALES_PRICE INT,
	DWH_CREATE_DATE DATETIME2 DEFAULT GETDATE()
);

-- ERP TABLES

-- ERP LOC_A101 TABLE

IF OBJECT_ID('[silver_Layer].erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE [silver_Layer].erp_loc_a101;
CREATE TABLE [silver_Layer].erp_loc_a101(
	CID NVARCHAR(50),
	CID_KEY NVARCHAR(50),
	COUNTRY NVARCHAR(50),
	DWH_CREATE_DATE DATETIME2 DEFAULT GETDATE()
);

-- ERP CUST_AZ12 TABLE

IF OBJECT_ID('[silver_Layer].erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE [silver_Layer].erp_cust_az12;
CREATE TABLE [silver_Layer].erp_cust_az12(
	CID NVARCHAR(50),
	BDATE DATE,
	GEN NVARCHAR(50),
	DWH_CREATE_DATE DATETIME2 DEFAULT GETDATE()
);

-- ERP PX_CAT_G1V2 TABLE

IF OBJECT_ID('[silver_Layer].erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE [silver_Layer].erp_px_cat_g1v2;
CREATE TABLE [silver_Layer].erp_px_cat_g1v2(
	ID NVARCHAR(50),
	CAT NVARCHAR(50),
	SUBCAT NVARCHAR(50),
	MAINTENANCE NVARCHAR(50),
	DWH_CREATE_DATE DATETIME2 DEFAULT GETDATE()
);
