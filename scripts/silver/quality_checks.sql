/*
Quality Check: primary key must be unique and not NULL
*/

SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;
-- there is an issue in this table, some duplicate pk are there
-- and 3 records where primary key = NULL

-- Check for unwanted spaces
SELECT cst_firstname 
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname);

-- thankfully, gender column has no leaading and trailing spaces

-- check the consistency of values in low cardinality columns
SELECT DISTINCT(cst_gndr)
FROM bronze.crm_cust_info;
	
SELECT DISTINCT(cst_marital_status) 
FROM bronze.crm_cust_info;


SELECT 
	prd_id,
	prd_key,
	SUBSTRING(prd_key, 1, 5) AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;

SELECT 
prd_id, 
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

SELECT 
	prd_id,
	prd_key,
	SUBSTRING(prd_key, 1, 5) AS cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info;

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2;
-- These are the same things, but in prd_info: CO-BF and in erp_px_cat_g1v2: AC_BC here underscore
-- Use REPLACE function

SELECT 
	prd_id,
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
SELECT sls_prd_key FROM bronze.crm_sales_details);

SELECT prd_cost
FROM bronze.crm_prd_info 
WHERE prd_cost <0 OR prd_cost IS NULL;

SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt< prd_start_dt;

-- sales_details

SELECT 
*
FROM 
bronze.crm_sales_details;
-- Check for invalid dates
SELECT
NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101;


GO
EXEC silver.load_silver;

SELECT * FROM silver.crm_sales_details;
