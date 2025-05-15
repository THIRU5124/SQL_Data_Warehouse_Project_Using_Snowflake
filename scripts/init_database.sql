/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


===============
SNOWFLAKE DDL's
===============


/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DATAWAREHOUSE'. 
    If the database exists, it is dropped and recreated.
    It also creates three schemas: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DATAWAREHOUSE' database if it exists. 
    All data in the database will be permanently deleted. 
    Make sure you have proper backups before executing.
=============================================================
*/

-- Drop and recreate the 'DATAWAREHOUSE' database
DROP DATABASE IF EXISTS DATAWAREHOUSE;

CREATE DATABASE DATAWAREHOUSE;

-- Switch to the new database
USE DATABASE DATAWAREHOUSE;

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

