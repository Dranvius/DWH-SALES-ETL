
-- GOLD


/*
	 Creacion de vistas
*/


--> All In procedure 

USE DWH_SALES_DB


 --> Usuarios
IF OBJECT_ID ('gold.DIM_CRM_INFO_CUSTOMER','V') IS NOT NULL
	DROP VIEW gold.DIM_CRM_INFO_CUSTOMER;
GO
CREATE VIEW gold.DIM_INFO_CRM_INFO_CUSTOMER AS 
(
SELECT 
	cst_id as customer_id,
	frist_name,
	last_name,
	CASE
		WHEN gender = 'n/a' THEN Genre
	ELSE gender END AS Gender,
	marital_status,
	cst_create_date as create_at,
	bird_date,
	DATEDIFF(YEAR, bird_date, GETDATE()) AS Age,
	Country
FROM DWH_SALES_DB.silver.CRM_CUSTOMER_INFO A 
LEFT JOIN DWH_SALES_DB.silver.ERP_CUSTOMER B
ON A.cst_id = B.id_customer
LEFT JOIN DWH_SALES_DB.silver.ERP_LOCALIDAD C
ON C.customer_id = A.cst_id
)


SELECT  * FROM DWH_SALES_DB.gold.DIM_INFO_CRM_INFO_CUSTOMER

--> Products

	-->VENTAS REALIZADAS
	-->FACT

	select min(created_at) from gold.FACT_CRM_SALES_INFO
	select max(created_at) from gold.FACT_CRM_SALES_INFO

	IF OBJECT_ID ('gold.FACT_CRM_SALES_INFO','V') IS NOT NULL
		DROP VIEW gold.FACT_CRM_SALES_INFO;
	GO
	CREATE VIEW gold.FACT_CRM_SALES_INFO AS (
	SELECT 
		ROW_NUMBER() OVER(ORDER BY sls_prd_key) as ID,
		sls_prd_key AS PK_ID_SALES,
		PK_KEY_SALES_INFO AS PK_KEY_PRODUCT_INFO,
		sls_cust_id AS FK_CUSTOMER_ID,
		sls_order_dt AS created_at,
		sls_ship_dt AS ship_date,
		sls_due_dt AS due_date,
		sls_quantity AS quantity,
		sls_sales_fomarted AS price_un,
		sls_price_fomarted AS total
	FROM DWH_SALES_DB.silver.CRM_SALES_INFO
	)

	

--> Crear vista Informacion_de_productos
	--> INFORMACION DE PRODUCTO
	--> CATEGORIA 
	--> PRECIO



	IF OBJECT_ID ('gold.DIM_CRM_INFO_PRODUCT','V') IS NOT NULL
		DROP VIEW gold.DIM_CRM_INFO_PRODUCT;
	GO
	CREATE VIEW gold.DIM_CRM_INFO_PRODUCT AS
	(
		SELECT 
			ROW_NUMBER() OVER(ORDER BY product_id) ID,
			product_id,
			crm_product_key_erp_product AS PK_ERP_PRODUCT,
			crm_product_key_sales_info AS PK_SALES_INFO,
			product_name,
			product_cost,
			product_line,
			CAT as categori,
			SUBCAT as sub_categori,
			product_start_date,
			product_end_date,
			MAINTENANCE as support_maintenance
		FROM DWH_SALES_DB.silver.CRM_PRODUCT_INFO a
		LEFT JOIN DWH_SALES_DB.silver.ERP_PRODUCTS b
		ON trim(a.crm_product_key_erp_product) = REPLACE(trim(b.ID),'_','-')
	)


	SELECT 
		*
	FROM gold.FACT_CRM_SALES_INFO A
	WHERE A.created_at = '0//0'
	
	28336
