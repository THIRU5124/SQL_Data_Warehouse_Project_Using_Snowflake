/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'gold.dim_customers'
-- ====================================================================
-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results 
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.product_key'
-- ====================================================================
-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results 
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- ====================================================================
-- Checking 'gold.fact_sales'
-- ====================================================================
-- Check the data model connectivity between fact and dimensions
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL OR c.customer_key IS NULL  


================
Snowflake Check
================

/*
===============================================================================
Gold Layer - Data Quality Checks
===============================================================================
Purpose:
    Validates data integrity, consistency, and analytical readiness of the 
    Gold Layer. Includes checks for uniqueness, referential integrity, nulls,
    logical correctness, and outliers.
===============================================================================
*/

-- =============================================================================
-- 1. Uniqueness Checks on Surrogate Keys (Expect: No Results)
-- =============================================================================

-- Duplicate customer_key in dim_customers
SELECT customer_key, COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;

-- Duplicate product_key in dim_products
SELECT product_key, COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;

-- =============================================================================
-- 2. Referential Integrity: Fact Table Keys Must Exist in Dimensions
-- =============================================================================
-- Expectation: No orphaned fact rows (all keys found in dimensions)

SELECT 
    f.order_number,
    f.customer_key,
    f.product_key
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE c.customer_key IS NULL OR p.product_key IS NULL;

-- =============================================================================
-- 3. Null Checks on Critical Columns (Expect: No Results)
-- =============================================================================

-- Missing customer_id in dim_customers
SELECT * FROM gold.dim_customers WHERE customer_id IS NULL;

-- Missing product_id in dim_products
SELECT * FROM gold.dim_products WHERE product_id IS NULL;

-- Missing order_number, sales, or quantity in fact_sales
SELECT * 
FROM gold.fact_sales
WHERE order_number IS NULL 
   OR sales_amount IS NULL
   OR quantity IS NULL;

-- =============================================================================
-- 4. Date Logic Checks (Expect: No Violations)
-- =============================================================================

-- Orders where ship date is before order date
SELECT * 
FROM gold.fact_sales
WHERE shipping_date < order_date;

-- Orders where due date is before order date
SELECT * 
FROM gold.fact_sales
WHERE due_date < order_date;

-- =============================================================================
-- 5. Outlier Detection (Manual Review Recommended)
-- =============================================================================

-- Unusually high prices
SELECT * 
FROM gold.fact_sales
WHERE price > 10000;

-- Negative or zero sales amounts
SELECT * 
FROM gold.fact_sales
WHERE sales_amount <= 0;

-- Extremely high order quantities
SELECT * 
FROM gold.fact_sales
WHERE quantity > 1000;

-- =============================================================================
-- End of Quality Checks
-- =============================================================================

