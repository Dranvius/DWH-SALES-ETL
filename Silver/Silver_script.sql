/*

Silver

*/


----
	--> USAR ESTA BASE DE DATOS

USE DWH_SALES_DB;

	--> Cleaning Data sets

--> Limpieza de data CRM_CUSTOMER_INFO 
	PRINT('Limpieza de CRM_CUSTOMER_INFO')

SELECT * 
FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO;

-- VALIDACIONES

	--> Verificar los valores unicos
	--> Existen valores duplicados a nivel de identificador unicos
	SELECT * 
	FROM(
		SELECT
			*,
			COUNT(cst_id) OVER (PARTITION BY cst_id) as Conteo
		FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO
	) a
	WHERE a.Conteo > 1 

	--> Comprobar que existan espacios en valores
	--> En primer nombre a.cst_firstname
SELECT 
	* 
FROM( 
SELECT 
	a.cst_firstname,
	TRIM(a.cst_firstname) AS SIN_SALTOS,
	LEN(TRIM(a.cst_firstname)) AS CONTEO_CON_TRIM,
	LEN(a.cst_firstname) AS CONTEO_SIN_TRIM
FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO a
GROUP BY a.cst_firstname,TRIM(a.cst_firstname)
) NAMES
WHERE NAMES.CONTEO_CON_TRIM != CONTEO_SIN_TRIM

	--> Comprobar que existan espacios en valores
	--> En primer nombre a.cst_lastname

SELECT 
	* 
FROM( 
SELECT 
	a.cst_lastname,
	TRIM(a.cst_lastname) AS SIN_SALTOS,
	LEN(TRIM(a.cst_lastname)) AS CONTEO_CON_TRIM,
	LEN(a.cst_lastname) AS CONTEO_SIN_TRIM
FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO a
GROUP BY a.cst_lastname,TRIM(a.cst_lastname)
) NAMES
WHERE NAMES.CONTEO_CON_TRIM != CONTEO_SIN_TRIM


	--> Estandarizacion de edades

SELECT 
	DISTINCT(cst_marital_status) 
FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO a	 

		--> S (Single)
		--> NULL (Nobody knows) 
		--> M (Marriage)

SELECT 
	DISTINCT(cst_gndr) 
FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO a

		--> NULL
		--> F (Female)
		--> M (Male)


	--> Fechas que no son nulos
	--> Fechas con formatos irregulares

	--> Why exist irregular dates ? 2025
	--> Current date = 2025 - 07 - 31

	SELECT 
		distinct(cst_create_date)
	FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO a
	WHERE year(cst_create_date) > 2025

	--> Why exist null dates

	SELECT 
		distinct(cst_create_date)
	FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO a
	WHERE cst_create_date is null


	-->  CST_KEY 
	-->  Existen valores duplicados
	SELECT 
		cst_key,
		count(cst_key) 
	FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO
	GROUP BY cst_key
	HAVING count(cst_key) > 1;

	SELECT * 
	FROM (
		SELECT 
			*,
			count(cst_key) OVER (PARTITION BY cst_key) as conteo
		FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO
		 ) A
		WHERE
		 A.conteo > 1

-- > Creacion de tablas para silver de DWH_SALES_DB.silver.CRM_CUSTOMER_INFO

SELECT 
	B.cst_id,
	B.cst_key,
	B.frist_name,
	B.last_name,
	B.gender,
	B.marital_status,
	B.cst_create_date
INTO DWH_SALES_DB.silver.CRM_CUSTOMER_INFO
FROM (
SELECT 
	ROW_NUMBER() OVER(PARTITION BY A.cst_id ORDER BY cst_create_date desc) as rn,
	A.cst_id,
	A.cst_key,
	TRIM(A.cst_firstname) AS frist_name,
	TRIM(A.cst_lastname) as last_name,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	ELSE 'n/a' END AS gender,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Marriage'
	ELSE 'n/a' END AS marital_status,
	cst_create_date 
FROM DWH_SALES_DB.bronce.CRM_CUSTOMER_INFO A 
WHERE cst_create_date IS NOT NULL 
) B
WHERE B.rn = 1


--> Limpieza de tabla CRM_PRODUCT_INFO 

PRINT('-->  LIMPIEZA DE TABLA CRM_PRODUCT_INFO ')

	SELECT * FROM DWH_SALES_DB.bronce.CRM_PRODUCT_INFO; 

	--> Verificar que las tablas no cuenten con PK duplicadas
		--> prd_id
		--> No existen duplicados

	SELECT
		prd_id,
		count(*) as conteo_valores
	FROM DWH_SALES_DB.bronce.CRM_PRODUCT_INFO 
	GROUP BY prd_id
	having count(*) > 1

	--> Verificacion de precios nulos
		--> Actividad correctiva (Buscar dato o establecr cero)

	SELECT
		*
	FROM DWH_SALES_DB.bronce.CRM_PRODUCT_INFO A
	WHERE A.prd_cost is null
	ORDER BY A.prd_nm asc; 

	--> Estandarizacion de prd_line 

	SELECT
		DISTINCT(prd_line),
		*
	FROM DWH_SALES_DB.bronce.CRM_PRODUCT_INFO A

	-- Null -> n/a
	-- M -> Mountain
	-- R -> Road
	-- S -> Sport
	-- T -> Touring

	
	--> Verificacion de fechas y busqueda de nulos
	
		--> Existen fechas de inicio que no estan muy bien configuradas
			--> Las fechas de inicio son posteriores a las fechas de finalizado el producto
			--> No existen fechas de inicio nulas
			--> Las fechas de final pueden ser nulas porque son productos que no se han finalizado
			--> Intervencion : Cambiar las dos fechas en este caso
			
	SELECT 
		* 
	FROM bronce.CRM_PRODUCT_INFO
	WHERE  1 = 1
	--AND	prd_start_dt > prd_end_dt
	AND prd_start_dt  IS NULL

	
	--> Construccion tabla Silver CRM_PRODUCT_INFO

	PRINT('---- > Crear tabla en esquema silver CRM_PRODUCT_INFO  ')


	SELECT 
		*
	INTO DWH_SALES_DB.silver.CRM_PRODUCT_INFO
	FROM
	(
	SELECT 
		prd_id as product_id,
		prd_key as product_key,
		LEFT(prd_key,5) AS crm_product_key_erp_product,
		RIGHT(trim(prd_key),7) AS crm_product_key_sales_info,
		prd_nm as product_name,
		prd_cost as product_cost,
		CASE
			WHEN TRIM(UPPER(prd_line)) = 'M' THEN 'Mountain'
			WHEN TRIM(UPPER(prd_line)) = 'R' THEN 'Road'
			WHEN TRIM(UPPER(prd_line)) = 'S' THEN 'Sport'
			WHEN TRIM(UPPER(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
		END AS product_line,
		CASE WHEN (TRY_CAST(prd_start_dt AS DATE) > TRY_CAST(prd_end_dt AS DATE)) AND prd_end_dt IS NOT NULL THEN prd_end_dt ELSE prd_start_dt END AS product_start_date,
		CASE WHEN TRY_CAST(prd_start_dt AS DATE) > TRY_CAST(prd_end_dt AS DATE) THEN prd_start_dt ELSE NULL END AS product_end_date
	FROM bronce.CRM_PRODUCT_INFO
	WHERE prd_cost IS NOT NULL
	) a


	--> Limpieza de tabla DWH_SALES_DB.bronce.CRM_SALES_INFO


	-- > Verificacion que tiene una pk y no este duplicada
	-- > Existen valores duplicados
	
	SELECT 
		sls_ord_num,
		count(*)
	FROM DWH_SALES_DB.bronce.CRM_SALES_INFO
	group by sls_ord_num
	having count(*) > 1
	
		--> por que se estan duplicando
		--> Se duplican por que existe mas de dos transacciones por usuario
		--> Establecer una pk para cada transaccion
		--> Establecer un ROW_NUMBER por fecha
		SELECT
			* 
		FROM(
			SELECT 
				*,
				count(sls_ord_num) OVER (PARTITION BY sls_ord_num)  AS COUN
			FROM DWH_SALES_DB.bronce.CRM_SALES_INFO
		) a
		where  COUN > 1

	-- > Verificacion de nulos
	
		--> a.sls_sales Existen nulos
		--> a.sls_price Existen nulos
		
		SELECT 
			* 
		FROM DWH_SALES_DB.bronce.CRM_SALES_INFO a 
		WHERE  1 = 1 	
		AND a.sls_sales IS NULL -- Nulos En sales
--		AND a.sls_price IS NULL -- Nulos en precios

				--> Formato para nulos

				SELECT 
						CASE WHEN sls_price IS NULL THEN sls_quantity * sls_sales ELSE sls_price END AS sls_price_fomarted,
						CASE WHEN sls_sales IS NULL THEN sls_quantity * sls_price ELSE sls_sales END AS sls_sales_fomarted
				FROM DWH_SALES_DB.bronce.CRM_SALES_INFO;   


	--> Establecer formatos para fechas 

		SELECT 
			RIGHT(a.sls_order_dt,2) + '/' + SUBSTRING(a.sls_order_dt,4,2) + '/' + LEFT(a.sls_order_dt,4) AS date_order,
			RIGHT(a.sls_ship_dt,2) + '/' + SUBSTRING(a.sls_ship_dt,4,2) + '/' + LEFT(a.sls_ship_dt,4) AS date_order,
			RIGHT(a.sls_ship_dt,2) + '/' + SUBSTRING(a.sls_ship_dt,4,2) + '/' + LEFT(a.sls_ship_dt,4) AS date_order
		FROM DWH_SALES_DB.bronce.CRM_SALES_INFO a 



	--> Creacion de tabla silver sales_info

		SELECT * FROM DWH_SALES_DB.bronce.CRM_SALES_INFO WHERE sls_ship_dt = '20302013'; 


	--> Definicion de tabla silver para CRM_SALES_INFO
		PRINT('------------------> CRM_SALES_INFO')

		SELECT
				sls_prd_key,
				PK_KEY_SALES_INFO,
				sls_cust_id,
				sls_order_dt,
				sls_ship_dt,
				sls_due_dt,
				sls_quantity,
				sls_price_fomarted,
				sls_sales_fomarted
		INTO DWH_SALES_DB.silver.CRM_SALES_INFO
		FROM
		(
			SELECT 
				count(sls_ord_num) OVER (PARTITION BY sls_ord_num) AS COUN,
				sls_prd_key,
				RIGHT(sls_prd_key,7) AS PK_KEY_SALES_INFO,
				sls_cust_id,
				RIGHT(sls_order_dt,2) + '/' + SUBSTRING(sls_order_dt,5,2) + '/' + LEFT(sls_order_dt,4) AS sls_order_dt,
				RIGHT(sls_ship_dt,2) + '/' + SUBSTRING(sls_ship_dt,5,2) + '/' + LEFT(sls_ship_dt,4) AS sls_ship_dt,
				RIGHT(sls_ship_dt,2) + '/' + SUBSTRING(sls_ship_dt,5,2) + '/' + LEFT(sls_ship_dt,4) AS sls_due_dt,
				sls_quantity,
				CASE WHEN sls_price IS NULL THEN sls_quantity * sls_sales ELSE sls_price END AS sls_price_fomarted,
				CASE WHEN sls_sales IS NULL THEN sls_quantity * sls_price ELSE sls_sales END AS sls_sales_fomarted
			FROM DWH_SALES_DB.bronce.CRM_SALES_INFO
			WHERE LEN(SLS_ORDER_DT) = 8
		) A 
		where  A.COUN > 1



--ERP SYSTEM  --------------------------------------->
	print(' ---------------------> ERP SOURCE')

		PRINT('-----------> DWH_SALES_DB.bronce.ERP_CUSTOMER ') 

	SELECT * FROM DWH_SALES_DB.bronce.ERP_CUSTOMER;

	-- > Verificacion de llaves nulas o duplicados
		--> NO EXISTEN NULOS
		--> NO EXISTEN DUPLICADOS
			SELECT 
				a.CID,
				count(*)
			FROM DWH_SALES_DB.bronce.ERP_CUSTOMER a
			GROUP BY a.CID
			HAVING count(*) > 1 

			SELECT 
				a.CID
			FROM DWH_SALES_DB.bronce.ERP_CUSTOMER a
			WHERE a.CID IS NULL
	
	--> ESTANDARIZACION DE GENERO
		--> No existe un genero estandarizado bien
		--> Establecer estandarizacion de generos


			SELECT 
				DISTINCT(GEN)
			FROM DWH_SALES_DB.bronce.ERP_CUSTOMER a;

			--NULL OR '' -> NULL
			--F -> FEMALE
			--FEMALE -> FEMALE
			--M --> MALE
			--MALE --> MALE

		--> Estandarizacion
			
			SELECT 
				DISTINCT(GENRE) 
			FROM(
			SELECT 
				TRIM(GEN) AS GENERO_DEFAULT,
				CASE 
					WHEN GEN IS NULL THEN 'n/a' 
					WHEN UPPER(TRIM(GEN)) = '' THEN 'n/a'
					WHEN UPPER(TRIM(GEN)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(GEN)) = 'Female' THEN 'Male'
					WHEN UPPER(TRIM(GEN)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(GEN)) = 'Male' THEN 'Male'
				ELSE 'n/a'
				END AS Genre
			FROM DWH_SALES_DB.bronce.ERP_CUSTOMER a
			) A 

		--> Separar la llave para tabla

		SELECT 
			RIGHT(a.CID,5) AS customer_id
		FROM DWH_SALES_DB.bronce.ERP_CUSTOMER a
			

		--> Creacion tabla  Silver

		SELECT * 
		INTO DWH_SALES_DB.SILVER.ERP_CUSTOMER
		FROM 
		(
			SELECT
				CID,
				RIGHT(CID,5) AS id_customer,
				bdate AS bird_date,
				CASE 
					WHEN GEN IS NULL THEN 'n/a' 
					WHEN UPPER(TRIM(GEN)) = '' THEN 'n/a'
					WHEN UPPER(TRIM(GEN)) = 'F' THEN 'Female'
					WHEN UPPER(TRIM(GEN)) = 'Female' THEN 'Male'
					WHEN UPPER(TRIM(GEN)) = 'M' THEN 'Male'
					WHEN UPPER(TRIM(GEN)) = 'Male' THEN 'Male'
				ELSE 'n/a'
				END AS Genre
			FROM DWH_SALES_DB.bronce.ERP_CUSTOMER
		) A 


		PRINT('-----------> DWH_SALES_DB.bronce.ERP_LOCALIDAD') 


	SELECT 
		* 
	FROM DWH_SALES_DB.bronce.ERP_LOCALIDAD

	--> Country estandarizado
	SELECT 
		DISTINCT(COUNTRY)
	FROM (
	SELECT 
		CASE
			WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
			WHEN TRIM(CNTRY) = 'USA' THEN 'United States'
			WHEN TRIM(CNTRY) = 'Germany' THEN 'Germany'
			WHEN TRIM(CNTRY) = 'United States' THEN 'United States'
			WHEN TRIM(CNTRY) IS NULL THEN 'n/a'
			WHEN TRIM(CNTRY) = '' THEN 'n/a'
			WHEN TRIM(CNTRY) = 'Australia' THEN 'Australia'
			WHEN TRIM(CNTRY) = 'United Kingdom' THEN 'United Kingdom'
			WHEN TRIM(CNTRY) = 'Canada' THEN 'Canada'
			WHEN TRIM(CNTRY) = 'France' THEN 'France'
			WHEN TRIM(CNTRY) = 'US' THEN 'United States'
		END COUNTRY
	FROM DWH_SALES_DB.bronce.ERP_LOCALIDAD
	) A


	--> Obtener el id de usuario

	SELECT 
		RIGHT(a.CID,5) AS customer_id
	FROM DWH_SALES_DB.bronce.ERP_LOCALIDAD a


	--> Carga de silver


	SELECT 
		* 
	INTO DWH_SALES_DB.SILVER.ERP_LOCALIDAD
	FROM 
	(
		SELECT 
			CID,
			RIGHT(CID,5) AS customer_id,
			CASE
				WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				WHEN TRIM(CNTRY) = 'USA' THEN 'United States'
				WHEN TRIM(CNTRY) = 'Germany' THEN 'Germany'
				WHEN TRIM(CNTRY) = 'United States' THEN 'United States'
				WHEN TRIM(CNTRY) IS NULL THEN 'n/a'
				WHEN TRIM(CNTRY) = '' THEN 'n/a'
				WHEN TRIM(CNTRY) = 'Australia' THEN 'Australia'
				WHEN TRIM(CNTRY) = 'United Kingdom' THEN 'United Kingdom'
				WHEN TRIM(CNTRY) = 'Canada' THEN 'Canada'
				WHEN TRIM(CNTRY) = 'France' THEN 'France'
				WHEN TRIM(CNTRY) = 'US' THEN 'United States'
			END COUNTRY
		FROM DWH_SALES_DB.bronce.ERP_LOCALIDAD	
	) A



		PRINT('-----------> DWH_SALES_DB.bronce.ERP_LOCALIDAD') 


	SELECT 
		* 
	FROM DWH_SALES_DB.bronce.ERP_PRODUCTS


	--> VERIFICACION DE NULOS
		--> NO TIENE NULOS

	SELECT 
		* 
	FROM DWH_SALES_DB.bronce.ERP_PRODUCTS
	WHERE
		1 = 1
--		AND	ID IS NULL	
--		AND	CAT IS NULL
--		AND	SUBCAT IS NULL
--		AND MAINTENANCE IS NULL


	--> IDENTIFICADOR DE DUPLICADOS EN PK
		--> No cuenta con duplicados 
	SELECT 
		ID,
		COUNT(*) AS conteo
	FROM DWH_SALES_DB.bronce.ERP_PRODUCTS
	GROUP BY ID
	HAVING COUNT(*) > 1 

	--> No cuenta con un identificador

	SELECT * FROM DWH_SALES_DB.bronce.ERP_PRODUCTS

	SELECT * 
	FROM DWH_SALES_DB.bronce.CRM_SALES_INFO A 
	INNER JOIN bronce.CRM_PRODUCT_INFO B
	ON right(trim(A.sls_prd_key),7) = right(trim(b.prd_key),7)

	SELECT * FROM  DWH_SALES_DB.bronce.CRM_SALES_INFO A
	
	SELECT * FROM DWH_SALES_DB.bronce.ERP_PRODUCTS 


	SELECT * 
	FROM bronce.CRM_PRODUCT_INFO A 
	INNER JOIN DWH_SALES_DB.bronce.ERP_PRODUCTS B
	ON LEFT(trim(A.prd_key),5) = REPLACE(trim(b.ID),'_','-')


	--> Creacion de tabla para silver 

	SELECT 
		*
	INTO DWH_SALES_DB.SILVER.ERP_PRODUCTS
	FROM 
	(
	SELECT 
		ROW_NUMBER() OVER(ORDER BY ID) AS id_table,
		*
	FROM DWH_SALES_DB.bronce.ERP_PRODUCTS
	) a 




	 