/*
=============================================================
CREATE DATABASE AND SCHEMAS
=============================================================
Script Purpose:
	This script first checks if the database 'LEDGERLINK_DWH' exists. 
	If it does, the database is dropped and then recreated. After creating the new database, 
	it sets up three schemas: 'Bronze_Layer','Silver_Layer' and 'Gold_Layer'.

Warnings:
	Executing this script will permanently delete the entire 'LEDGERLINK_DWH' database if it exists. 
	All data will be lost. Please proceed with caution and ensure that proper backups 
	are in place before running the script.
*/
USE MASTER;
GO 

-- Drop and Recreate "LedgerLink_DWH" database
IF EXISTS (SELECT 1 FROM SYS.DATABASES WHERE NAME = 'LedgerLink_DWH')
BEGIN
	ALTER DATABASE LedgerLink_DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE LedgerLink_DWH;
END;
GO

CREATE DATABASE LedgerLink_DWH;
GO

USE LedgerLink_DWH;
GO

CREATE SCHEMA Bronze_Layer;
GO
CREATE SCHEMA Silver_Layer;
GO
CREATE SCHEMA Gold_Layer;
GO
