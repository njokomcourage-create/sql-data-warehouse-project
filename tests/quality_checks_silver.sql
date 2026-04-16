============================================================================
  QUALITY CHECK ON silver.crm_cust_info
==============================================================================

  --CHECK FOR DUPLICATES OR NULLS IN CUSTOMER_ID
  SELECT
  cst_id,
  COUNT(*)
  FROM silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1 OR cst_id IS NULL

--CHECK FOR UNWANTED SPACES
--EXPECTATION: NO RESULTS
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--DATA STANDARDIZATION  & CONSISTENCY CHECK ON marital_status and gender
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info


============================================================================
  QUALITY CHECK ON silver.crm_prd_info
==============================================================================
-check for duplicates in primary key (prd_id)
SELECT
  prd_id,
  COUNT(*)
  FROM silver.crm_prd_info
  GROUP BY prd_id
  HAVING COUNT(*) > 1 OR prd_id IS NULL

--CHECK FOR UNWANTED SPACES
--EXPECTATION: NO RESULTS
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--CHECK PRODUCT COSTS FOR NULLS AND NEGATIVE NUMBERS
SELECT
prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0

-- CHECK FOR INVALID DATE Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

