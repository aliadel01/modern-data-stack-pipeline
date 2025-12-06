-- ===============================
-- Create Database and Schemas in Snowflake
-- ===============================

-- Create warehouse (if not exists)
CREATE WAREHOUSE IF NOT EXISTS etl_wh
  WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE etl_wh;

-- Drop database if exists (CAUTION: deletes all data)
DROP DATABASE IF EXISTS sales_dwh;

-- Create database
CREATE DATABASE sales_dwh;
USE DATABASE sales_dwh;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
