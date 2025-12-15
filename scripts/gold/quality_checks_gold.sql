-- check quality of gold table
SELECT * FROM gold.dim_customers;
SELECT DISTINCT gender FROM gold.dim_customers;
-- check for duplicates prod dim:
SELECT prd_key, COUNT(*) FROM(
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id 
WHERE prd_end_dt is NULL
) AS t GROUP BY prd_key
HAVING COUNT(*) > 1;


SELECT * FROM gold.dim_products;
-- Check quality of the gold tables
-- Check if all dim tables can successfully join to the fact table
SELECT *
FROM gold.fact_sales AS f
LEFT JOIN gold.dim_customers AS c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products AS p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL;

