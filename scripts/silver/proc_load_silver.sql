--STORED PROCEDURE FOR SILVER LAYER
--INSERTING silver.crm_cust_info
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
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
    TRIM(cst_firstname) AS cst_firstname,  --REMOVED UNWANTED SPACES FROM FIRSTNAME AND LAST NAME AS NEXTCOMMAND
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
        ELSE 'n\a'
    END cst_marital_status, --NORMALLIZE MARITAL STATUS TO READABLE FORMAT
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
        ELSE 'n\a'
    END cst_gndr, --NORMALLIZE GENDER VALUES TO READABLE FORMAT
    cst_create_date
    FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
    )t WHERE flag = 1 --SELECT THE MOST RECENT RECORD PER USER

    --INSERTING FOR silver.crm_prd_info
    TRUNCATE TABLE silver.crm_prd_info
    INSERT INTO silver.crm_prd_info(
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
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'), 
    SUBSTRING(prd_key, 7, LEN(prd_key)),
    prd_nm,
    COALESCE(prd_cost, 0),  --REPLACE THE NULLS IN THE PRODUCT COST WITH 0
    CASE UPPER(TRIM(prd_line)) 
    WHEN 'M' THEN 'MOUNTAIN'  
    WHEN 'R' THEN 'ROAD'
    WHEN 'S' THEN 'OTHER SALES'
    WHEN 'T' THEN 'TOURING'
    ELSE 'n/a'
    END prd_line,
    CAST(prd_start_dt AS DATE),
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE)
    FROM bronze.crm_prd_info



    --INSERTING FOR silver.crm_sales_details
    TRUNCATE TABLE silver.crm_sales_details
    INSERT INTO silver.crm_sales_details(
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_quantity,
    sls_price,
    sls_sales
    )
    SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,    
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt,
    sls_quantity,
    CASE WHEN sls_sales <= 0 or sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price) 
        ELSE sls_sales 
    END AS sls_sales,  --if sales is nagative,zero or null,derive it using quantity and price  
    CASE WHEN sls_price = 0 OR sls_price IS NULL
            THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price    
    FROM bronze.crm_sales_details


    

    --INSERTING FOR silver.erp_cust_az12
    TRUNCATE TABLE silver.erp_cust_az12
    INSERT INTO silver.erp_cust_az12(
        cid,
        bdate,
        gen
    )
    SELECT 
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))  --REMOVE 'NAS' PREFIX IF PRESENT
        ELSE cid
    END cid,
    CASE WHEN bdate > GETDATE() THEN NULL -- SET FUTURE BIRTHDATES TO NULL
        ELSE bdate
    END AS bdate,
    CASE 
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('F', 'FEMALE') 
            THEN 'FEMALE'
        WHEN UPPER(TRIM(REPLACE(REPLACE(gen, CHAR(13), ''), CHAR(10), ''))) IN ('M', 'MALE') 
            THEN 'MALE'
        ELSE 'n/a'
    END AS gen   --NORMALLIZE GENDER VALUES AND HANDLE UNKNOWN CASES
    FROM bronze.erp_cust_az12


    --INSERTING FOR silver.erp_loc_a101 
    TRUNCATE TABLE silver.erp_loc_a101
    INSERT INTO silver.erp_loc_a101(
        cid,
        cntry
    )
    SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) = 'DE' 
            THEN 'GERMANY'
        WHEN UPPER(TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))) IN ('US', 'USA') 
            THEN 'UNITED STATES'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) IS NULL 
                OR TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')) = '' 
            THEN 'n/a'
            ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), ''))
        END AS cntry   --NORMALIZE ,HANDLE MISSING OR BLANK COUNTRY CODES,AND HANDLED CARRIAGE RETURNS    
    FROM bronze.erp_loc_a101


    --INSERTING FOR silver.erp_px_cat_g1v2
    TRUNCATE TABLE silver.erp_px_cat_g1v2
    INSERT INTO silver.erp_px_cat_g1v2(
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
    FROM bronze.erp_px_cat_g1v2
END




