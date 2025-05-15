/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from staged CSV files. 
    It performs the following actions:
    - Truncates the target bronze tables before loading.
    - Uses the `COPY INTO` command to load data from internal stages into Snowflake tables.

Pre-requisites:
    - CSV files must be uploaded to the internal stages associated with their target tables.
    - File format: CSV, header row skipped, fields optionally enclosed by quotes.

Parameters:
    None. 
    This stored procedure does not accept any parameters.

Usage Example:
    CALL bronze.load_bronze();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    -- Start timer
    SELECT CURRENT_TIMESTAMP INTO batch_start_time;

    -- CRM_CUST_INFO
    SELECT CURRENT_TIMESTAMP INTO start_time;
    TRUNCATE TABLE bronze.crm_cust_info;
    COPY INTO bronze.crm_cust_info
    FROM @bronze.bronze_stage/cust_info.csv.gz
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    SELECT CURRENT_TIMESTAMP INTO end_time;

    -- CRM_PRD_INFO
    SELECT CURRENT_TIMESTAMP INTO start_time;
    TRUNCATE TABLE bronze.crm_prd_info;
    COPY INTO bronze.crm_prd_info
    FROM @bronze.bronze_stage/prd_info.csv.gz
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    SELECT CURRENT_TIMESTAMP INTO end_time;

    -- CRM_SALES_DETAILS
    SELECT CURRENT_TIMESTAMP INTO start_time;
    TRUNCATE TABLE bronze.crm_sales_details;
    COPY INTO bronze.crm_sales_details
    FROM @bronze.bronze_stage/sales_details.csv.gz
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    SELECT CURRENT_TIMESTAMP INTO end_time;

    -- ERP_LOC_A101
    SELECT CURRENT_TIMESTAMP INTO start_time;
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY INTO bronze.erp_loc_a101
    FROM @bronze.bronze_stage/LOC_A101.csv.gz
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    SELECT CURRENT_TIMESTAMP INTO end_time;

    -- ERP_CUST_AZ12
    SELECT CURRENT_TIMESTAMP INTO start_time;
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY INTO bronze.erp_cust_az12
    FROM @bronze.bronze_stage/CUST_AZ12.csv.gz
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    SELECT CURRENT_TIMESTAMP INTO end_time;

    -- ERP_PX_CAT_G1V2
    SELECT CURRENT_TIMESTAMP INTO start_time;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY INTO bronze.erp_px_cat_g1v2
    FROM @bronze.bronze_stage/PX_CAT_G1V2.csv.gz
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
    SELECT CURRENT_TIMESTAMP INTO end_time;

    -- End timer
    SELECT CURRENT_TIMESTAMP INTO batch_end_time;

    RETURN 'Bronze layer loaded successfully in ' || DATEDIFF('second', batch_start_time, batch_end_time) || ' seconds';
END;
$$;


CALL bronze.load_bronze();
