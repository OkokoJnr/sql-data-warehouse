--=============================================================
--Create Database 'DataWarehouse' and schemas 'bronze', 'silver', 'gold'
--=============================================================
-- Script Purpose:
--     This script create and initializes the database and creates the necessary schemas.

--WARNING: This script will drop the existing 'DataWarehouse' database if it exists.
--Make sure to back up any important data before running this script.
--=============================================================

DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
