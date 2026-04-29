/*
===========================================================
Gold Layer - Data Quality Checks
===========================================================

Purpose:
- Validate data quality and integrity of Gold layer views
- Ensure dimensions and fact tables are properly connected
- Confirm readiness for reporting and analytics

Checks Covered:
1. Surrogate Key Uniqueness (Dimensions)
2. Fact-Dimension Relationship Integrity
3. Missing / Broken References

===========================================================
*/


-----------------------------------------------------------
-- 1. dim_customers - Surrogate Key Validation
-----------------------------------------------------------

-- Check for duplicate customer_key
-- Expectation: No duplicate records
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;



-----------------------------------------------------------
-- 2. dim_products - Surrogate Key Validation
-----------------------------------------------------------

-- Check for duplicate product_key
-- Expectation: No duplicate records
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;



-----------------------------------------------------------
-- 3. fact_sales - Data Model Integrity Check
-----------------------------------------------------------

-- Validate relationship between fact and dimensions
-- Ensures all fact records have matching dimension keys
-- Expectation: No NULLs in joined dimension keys

SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
    ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
    ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;



-----------------------------------------------------------
-- End of Gold Layer Data Quality Checks
-----------------------------------------------------------
-- If all queries return expected results:
-- ✔ Data model is valid and ready for reporting
-----------------------------------------------------------
