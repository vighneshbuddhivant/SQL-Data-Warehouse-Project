```sql
/*
===========================================================
Silver Layer Transformation: crm_cust_info
===========================================================

Purpose:
- Clean and transform raw customer data from Bronze layer
- Load high-quality, deduplicated data into Silver layer

Steps Performed:
1. Data Exploration
   - View raw data from bronze.crm_cust_info

2. Data Quality Check (Primary Key Validation)
   - Compare total count vs distinct count of cst_id
   - Identify duplicates and NULL values in primary key

3. Duplicate Handling
   - Use ROW_NUMBER() to keep latest record per cst_id
   - Based on cst_create_date (latest record retained)

4. Data Cleaning
   - Remove unwanted spaces using TRIM() for names

5. Data Standardization
   - Convert coded values into meaningful values:
     cst_gndr:
        M → Male
        F → Female
        NULL/others → N/A

     cst_marital_status:
        M → Married
        S → Single
        NULL/others → N/A

6. Filtering
   - Remove records where cst_id IS NULL

7. Load into Silver Layer
   - Insert cleaned and transformed data into silver.crm_cust_info

8. Post-Load Validation
   - Ensure no duplicates
   - Ensure no NULL primary keys
   - Ensure no unwanted spaces remain

===========================================================
*/

-- Step 1: View raw data
SELECT * FROM bronze.crm_cust_info;

-- Step 2: Check for duplicates and NULLs in primary key
SELECT COUNT(cst_id) AS Total_Count, 
       COUNT(DISTINCT cst_id) AS Total_distinct_count
FROM bronze.crm_cust_info;

-- Step 3: Identify duplicate or NULL primary keys
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Step 4: Keep latest record using ROW_NUMBER
SELECT * 
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
    FROM bronze.crm_cust_info
) t
WHERE flag_last = 1;

-- Step 5: Check unwanted spaces
SELECT cst_firstname 
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Step 6: Clean and transform data
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'N/A'
    END AS cst_marital_status,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'N/A'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

-- Step 7: Load into Silver table
INSERT INTO silver.crm_cust_info 
(cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        ELSE 'N/A'
    END,
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'N/A'
    END,
    cst_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last 
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

-- Step 8: Post-load validation

-- Check counts
SELECT COUNT(cst_id) AS Total_Count, 
       COUNT(DISTINCT cst_id) AS Total_distinct_count
FROM silver.crm_cust_info;

-- Check duplicates
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check unwanted spaces
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Final data view
SELECT * FROM silver.crm_cust_info;
```
