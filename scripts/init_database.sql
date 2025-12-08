/* 
----------------------------------------------------
Create Database and Schema
----------------------------------------------------
Script Purpose:
  The following script is used to create a new database called as 'DataWarehouse' after checking if it already exists.
  If the database already exists, it is dropped and then created again.
  Inside this database, we create three schemas, namely, bronze, silver, and gold.

WARNING: 
  Running this script will drop the entire database 'DataWarehouse' (if it exists)
  All data in the database will be permanantly deleted. Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- Create database:'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create schema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
-- GO separates batches when working with multiple SQL statements.


