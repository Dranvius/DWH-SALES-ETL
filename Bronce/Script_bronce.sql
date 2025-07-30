-- Bronce Proyect

-- Lectura de proyectos

-- Creacion de bases de datos
CREATE DATABASE DWH_SALES_DB;

-- Usar una base de datos

USE DWH_SALES_DB

-- Creacion de esquema 
	-- Bronce
	-- Silver
	-- Gold

--CREATE SCHEMA bronce;
--GO
--CREATE SCHEMA silver;
--GO
--CREATE SCHEMA gold;
--GO

-- Procesos almacenados

DROP PROCEDURE bronce_layer
CREATE PROCEDURE bronce_layer AS
BEGIN
		-- Creacion de tablas

		-- Customer
		-- CRM



			PRINT '>> Create Table IF NOT EXISTS  CRM_CUSTOMER_INFO ---------------------'

		IF OBJECT_ID('bronce.CRM_CUSTOMER_INFO','U') IS NOT NULL
			DROP TABLE bronce.CRM_CUSTOMER_INFO;
		CREATE TABLE bronce.CRM_CUSTOMER_INFO(
			cst_id INT,
			cst_key VARCHAR(100),
			cst_FIRSTROWname VARCHAR(100),
			cst_lastname VARCHAR(100),
			cst_marital_status VARCHAR(10),
			cst_gndr VARCHAR(10),
			cst_create_date DATE
		)



		PRINT '>> TRUNCATE TABLE CRM_CUSTOMER_INFO ---------------------'

		TRUNCATE TABLE bronce.CRM_CUSTOMER_INFO; 

		PRINT '>> LOAD TABLE CRM_CUSTOMER_INFO ---------------------'

		BULK INSERT bronce.CRM_CUSTOMER_INFO
		FROM 'C:\Users\LENOVO\Documents\DWH_SALES\Bronce\Materials\datasets\source_crm\cust_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)


		-- Informacion de producto


		PRINT '>> Create Table IF NOT EXISTS  CRM_PRODUCT_INFO ---------------------'

		IF OBJECT_ID('bronce.CRM_PRODUCT_INFO','U') IS NOT NULL
			DROP TABLE bronce.CRM_PRODUCT_INFO;
		CREATE TABLE bronce.CRM_PRODUCT_INFO(
				prd_id INT,
				prd_key VARCHAR(100),
				prd_nm VARCHAR(100),
				prd_cost INT,
				prd_line VARCHAR(10),
				prd_start_dt DATE,
				prd_end_dt DATE
		)

		PRINT '>> Truncate table CRM_PRODUCT_INFO ---------------------'

		TRUNCATE TABLE bronce.CRM_PRODUCT_INFO; 

		PRINT '>> LOAD CRM_PRODUCT_INFO ---------------------'

		BULK INSERT bronce.CRM_PRODUCT_INFO
		FROM 'C:\Users\LENOVO\Documents\DWH_SALES\Bronce\Materials\datasets\source_crm\prd_info.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)


		-- Detalles de venta

		PRINT '>> Create Table IF NOT EXISTS  bronce.CRM_SALES_INFO ---------------------'

		IF OBJECT_ID('bronce.CRM_SALES_INFO','U') IS NOT NULL
			DROP TABLE bronce.CRM_SALES_INFO;
		CREATE TABLE bronce.CRM_SALES_INFO(
			sls_ord_num VARCHAR(100),
			sls_prd_key VARCHAR(100),
			sls_cust_id INT,
			sls_order_dt VARCHAR(50), --Fecha mal formateada
			sls_ship_dt VARCHAR(50), --Fecha mal formateada
			sls_due_dt VARCHAR(50), --Fecha mal formateada
			sls_sales INT,
			sls_quantity INT,
			sls_price INT
		)

		PRINT '>> TRUNCATE CRM_SALES_INFO ---------------------'
		TRUNCATE TABLE bronce.CRM_SALES_INFO; 

		PRINT '>> LOAD bronce.CRM_SALES_INFO ---------------------'

		BULK INSERT bronce.CRM_SALES_INFO
		FROM 'C:\Users\LENOVO\Documents\DWH_SALES\Bronce\Materials\datasets\source_crm\sales_details.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)


		-- ERP

		PRINT '>> Create Table IF NOT EXISTS ERP_LOCALIDAD---------------------'

		-- Locación
		IF OBJECT_ID('bronce.ERP_LOCALIDAD','U') IS NOT NULL
			DROP TABLE bronce.ERP_LOCALIDAD;
		CREATE TABLE bronce.ERP_LOCALIDAD(
			CID VARCHAR(100),
			CNTRY VARCHAR(100)
		)

		PRINT '>> TRUNCATE ERP_LOCALIDAD--------------------->'
		TRUNCATE TABLE bronce.ERP_LOCALIDAD;

		PRINT '>> LOAD EXISTS ERP_LOCALIDAD--------------------->'
		BULK INSERT bronce.ERP_LOCALIDAD
		FROM 'C:\Users\LENOVO\Documents\DWH_SALES\Bronce\Materials\datasets\source_erp\LOC_A101.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)


		-- Productos


		PRINT '>> Create Table IF NOT EXISTS bronce.ERP_PRODUCTS---------------------'
		IF OBJECT_ID('bronce.ERP_PRODUCTS','U') IS NOT NULL
			DROP TABLE bronce.ERP_PRODUCTS;
		CREATE TABLE bronce.ERP_PRODUCTS(
			ID VARCHAR(30),
			CAT VARCHAR(100),
			SUBCAT VARCHAR(100),
			MAINTENANCE VARCHAR(10)
		)


		PRINT '>> TRUNCATE ERP_PRODUCTS---------------------'
		TRUNCATE TABLE bronce.ERP_PRODUCTS;

		PRINT '>> LOAD ERP_PRODUCTS---------------------'
		BULK INSERT bronce.ERP_PRODUCTS
		FROM 'C:\Users\LENOVO\Documents\DWH_SALES\Bronce\Materials\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)


		-- Customers

		PRINT '>> Create Table IF NOT EXISTS ERP_CUSTOMER---------------------'
		IF OBJECT_ID('bronce.ERP_CUSTOMER','U') IS NOT NULL
			DROP TABLE bronce.ERP_CUSTOMER; 
		CREATE TABLE bronce.ERP_CUSTOMER(
			CID VARCHAR(50),
			BDATE DATE, 
			GEN VARCHAR(10)
		)

		PRINT '>> TRUNCATE ERP_CUSTOMER---------------------'
		TRUNCATE TABLE bronce.ERP_CUSTOMER;


		PRINT '>> LOAD ERP_CUSTOMER---------------------'
		BULK INSERT bronce.ERP_CUSTOMER
		FROM 'C:\Users\LENOVO\Documents\DWH_SALES\Bronce\Materials\datasets\source_erp\CUST_AZ12.csv'
		WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK)
END

EXEC bronce_layer;

 
