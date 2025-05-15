/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Returns:
    A string message confirming successful load and duration in seconds.

Usage Example:
    CALL silver.load_silver();
===============================================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    -- Record start time of full batch
    SELECT CURRENT_TIMESTAMP INTO batch_start_time;

    --------------------------------------------------------------------------------
    -- Load CRM Tables
    --------------------------------------------------------------------------------

    -- Load silver.crm_cust_info
    TRUNCATE TABLE silver.crm_cust_info;
    INSERT INTO silver.crm_cust_info (
        cst_id, 
        cst_key, 
        cst_firstname, 
        cst_lastname, 
        cst_marital_status, 
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        -- Normalize marital status values
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END,
        -- Normalize gender values
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END,
        cst_create_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS sub
    WHERE rn = 1; -- Keep most recent record per customer

    -- Load silver.crm_prd_info
    TRUNCATE TABLE silver.crm_prd_info;
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTR(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
        SUBSTR(prd_key, 7) AS prd_key,                      -- Extract product key
        prd_nm,
        COALESCE(prd_cost, 0),                              -- Default cost to 0 if null
        -- Map product line codes to descriptive values
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END,
        CAST(prd_start_dt AS DATE),
        -- Calculate end date as one day before the next start date
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) 
            - INTERVAL '1 DAY' AS DATE
        )
    FROM bronze.crm_prd_info;

    -- Load silver.crm_sales_details
    TRUNCATE TABLE silver.crm_sales_details;
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT 
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        -- Convert order date from int format to DATE
        TRY_TO_DATE(TO_VARCHAR(sls_order_dt)) AS sls_order_dt,
        TRY_TO_DATE(TO_VARCHAR(sls_ship_dt)) AS sls_ship_dt,
        TRY_TO_DATE(TO_VARCHAR(sls_due_dt)) AS sls_due_dt,
        -- Recalculate sales if original is missing or incorrect
        CASE 
            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        -- Derive price if original is null or invalid
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    --------------------------------------------------------------------------------
    -- Load ERP Tables
    --------------------------------------------------------------------------------

    -- Load silver.erp_cust_az12
    TRUNCATE TABLE silver.erp_cust_az12;
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT
        -- Remove 'NAS' prefix if present
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4)
            ELSE cid
        END,
        -- Remove future birthdates
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        -- Normalize gender values and handle unknowns
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END
    FROM bronze.erp_cust_az12;

    -- Load silver.erp_loc_a101
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT
        REPLACE(cid, '-', ''), -- Remove dashes from CID
        -- Normalize country values
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    -- Load silver.erp_px_cat_g1v2
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;

    -- Record end time and return success message
    SELECT CURRENT_TIMESTAMP INTO batch_end_time;
    RETURN 'Silver layer loaded successfully in ' || DATEDIFF('second', batch_start_time, batch_end_time) || ' seconds';
END;
$$;


CALL silver.load_silver();