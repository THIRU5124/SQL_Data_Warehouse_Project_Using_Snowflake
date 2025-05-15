
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
