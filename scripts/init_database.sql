/*
============================================
Data Warehouse Setup (Medallion Architecture)
============================================

Step 1: USE master
- Switch to the system database
- Required to create a new database

Step 2: CREATE DATABASE Datawarehouse
- Creates a central data warehouse
- Will contain Bronze, Silver, and Gold layers

Step 3: USE Datawarehouse
- Switch context to the newly created database
- All schemas and tables will be created here

--------------------------------------------
Schemas (Medallion Architecture Layers)
--------------------------------------------

Bronze Schema:
- Stores raw data from source systems
- No transformation applied
- Used for data backup, auditing, and reprocessing

Silver Schema:
- Stores cleaned and transformed data
- Handles duplicates, null values, and formatting
- Acts as an intermediate processing layer

Gold Schema:
- Stores business-ready data
- Aggregated and optimized for reporting
- Used by BI tools, dashboards, and analytics

============================================
*/

USE master;

CREATE DATABASE Datawarehouse;

USE Datawarehouse;

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
