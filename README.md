# Proyecto DWH_SALES_DB - Capa Bronce

## Descripción

La **fase Bronce** del proyecto corresponde a la **ingesta inicial de datos crudos** provenientes de sistemas **CRM** y **ERP**.  
Estos datos se cargan en un **Data Warehouse** bajo el esquema `bronce` y se organizan en tablas específicas sin aplicar transformaciones complejas, manteniendo el formato original de las fuentes.

---

## Creación de Base de Datos y Esquemas

Se define una base de datos principal y tres esquemas para soportar las distintas fases del pipeline:

```sql
CREATE DATABASE DWH_SALES_DB;
USE DWH_SALES_DB;

-- Esquemas de trabajo
CREATE SCHEMA bronce;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

```

Procedimiento Almacenado: bronce_layer

Se implementa el procedimiento almacenado bronce_layer encargado de:

Crear tablas en el esquema bronce.

Truncar tablas (en caso de existir datos previos).

Cargar datos con BULK INSERT desde archivos .csv.

Ejemplo de ejecución:

EXEC bronce_layer;


Tablas creadas en la Capa Bronce
1. CRM_CUSTOMER_INFO

Contiene información básica de clientes provenientes del CRM.

Columna	Tipo de Dato	Descripción
cst_id	INT	Identificador del cliente
cst_key	VARCHAR(100)	Llave única del cliente
cst_firstname	VARCHAR(100)	Nombre
cst_lastname	VARCHAR(100)	Apellido
cst_marital_status	VARCHAR(10)	Estado civil
cst_gndr	VARCHAR(10)	Género
cst_create_date	DATE	Fecha de creación en CRM




2. CRM_PRODUCT_INFO

Contiene información de productos registrados en CRM.

Columna	Tipo de Dato	Descripción
prd_id	INT	Identificador del producto
prd_key	VARCHAR(100)	Llave del producto
prd_nm	VARCHAR(100)	Nombre del producto
prd_cost	INT	Costo asociado
prd_line	VARCHAR(10)	Línea de producto
prd_start_dt	DATE	Fecha de inicio
prd_end_dt	DATE	Fecha de fin


3. CRM_SALES_INFO

Detalles de ventas registradas en CRM.

Columna	Tipo de Dato	Descripción
sls_ord_num	VARCHAR(100)	Número de orden de venta
sls_prd_key	VARCHAR(100)	Producto vendido
sls_cust_id	INT	Cliente asociado
sls_order_dt	VARCHAR(50)	Fecha de orden (mal formateada)
sls_ship_dt	VARCHAR(50)	Fecha de envío (mal formateada)
sls_due_dt	VARCHAR(50)	Fecha de vencimiento (mal formateada)
sls_sales	INT	Valor de la venta
sls_quantity	INT	Cantidad vendida
sls_price	INT	Precio unitario

4. ERP_LOCALIDAD

Contiene información de localización de clientes y operaciones.

Columna	Tipo de Dato	Descripción
CID	VARCHAR(100)	Identificador
CNTRY	VARCHAR(100)	País

5. ERP_PRODUCTS

Catálogo de productos del ERP.

Columna	Tipo de Dato	Descripción
ID	VARCHAR(30)	Identificador producto
CAT	VARCHAR(100)	Categoría
SUBCAT	VARCHAR(100)	Subcategoría
MAINTENANCE	VARCHAR(10)	Indicador mantenimiento

6. ERP_CUSTOMER

Información de clientes en ERP.

Columna	Tipo de Dato	Descripción
CID	VARCHAR(50)	Identificador cliente
BDATE	DATE	Fecha de nacimiento
GEN	VARCHAR(10)	Género


**Silver**

# Proyecto DWH_SALES_DB - Capa Silver

## Descripción

La **fase Silver** corresponde a la etapa de **limpieza, estandarización y validación de datos**.  
En esta capa, se parte de los datos crudos almacenados en la capa **Bronce** y se aplican reglas de calidad para generar tablas consistentes y listas para análisis en la capa **Gold**.

---

## Objetivos de la Capa Silver

- Eliminar duplicados.  
- Corregir y estandarizar valores.  
- Validar integridad de claves primarias.  
- Estandarizar formatos de fechas, nombres y categorías.  
- Crear nuevas tablas en el esquema `silver` listas para análisis.  

---

## Tablas Procesadas

### 1. `CRM_CUSTOMER_INFO`

**Acciones realizadas:**
- Eliminación de duplicados usando `ROW_NUMBER()`.  
- Eliminación de espacios en blanco en nombres y apellidos (`TRIM`).  
- Estandarización de género:
  - `M` → `Male`  
  - `F` → `Female`  
  - Otros/nulos → `n/a`  
- Estandarización de estado civil:
  - `S` → `Single`  
  - `M` → `Marriage`  
  - Otros/nulos → `n/a`  
- Filtrado de fechas inválidas o nulas (`cst_create_date`).  

**Tabla final:** `DWH_SALES_DB.silver.CRM_CUSTOMER_INFO`

| Columna          | Descripción               |
|------------------|---------------------------|
| cst_id           | Identificador único       |
| cst_key          | Llave de cliente          |
| first_name       | Nombre (sin espacios)     |
| last_name        | Apellido (sin espacios)   |
| gender           | Género estandarizado      |
| marital_status   | Estado civil estandarizado|
| cst_create_date  | Fecha de creación válida  |

---

### 2. `CRM_PRODUCT_INFO`

**Acciones realizadas:**
- Validación de `prd_id` como PK sin duplicados.  
- Revisión de precios (`prd_cost`), reemplazo o imputación de valores nulos.  
- Estandarización de `prd_line`:
  - `M` → `Mountain`  
  - `R` → `Road`  
  - `S` → `Sport`  
  - `T` → `Touring`  
  - Nulos → `n/a`  
- Ajuste de fechas inválidas (`prd_start_dt` > `prd_end_dt`).  
- Creación de llaves de integración con otras tablas (`LEFT` y `RIGHT` de `prd_key`).  

**Tabla final:** `DWH_SALES_DB.silver.CRM_PRODUCT_INFO`

| Columna                  | Descripción                          |
|---------------------------|--------------------------------------|
| product_id               | ID único del producto                |
| product_key              | Llave del producto                   |
| crm_product_key_erp_product | Llave parcial para unión con ERP   |
| crm_product_key_sales_info | Llave parcial para unión con ventas |
| product_name             | Nombre del producto                  |
| product_cost             | Costo del producto                   |
| product_line             | Línea estandarizada                  |
| product_start_date       | Fecha inicio válida                  |
| product_end_date         | Fecha fin (si aplica)                |

---

### 3. `CRM_SALES_INFO`

**Acciones realizadas:**
- Validación de duplicados en `sls_ord_num`.  
- Creación de PK artificial con `ROW_NUMBER()` para transacciones duplicadas.  
- Estandarización de fechas (`dd/mm/yyyy`).  
- Corrección de nulos en:
  - `sls_price`: calculado como `sls_quantity * sls_sales`.  
  - `sls_sales`: calculado como `sls_quantity * sls_price`.  

**Tabla final:** `DWH_SALES_DB.silver.CRM_SALES_INFO`

| Columna             | Descripción                           |
|----------------------|---------------------------------------|
| sls_prd_key          | Llave de producto                    |
| PK_KEY_SALES_INFO    | PK generada para ventas               |
| sls_cust_id          | Cliente                              |
| sls_order_dt         | Fecha de orden estandarizada          |
| sls_ship_dt          | Fecha de envío estandarizada          |
| sls_due_dt           | Fecha de vencimiento estandarizada    |
| sls_quantity         | Cantidad                             |
| sls_price_formatted  | Precio corregido                     |
| sls_sales_formatted  | Venta corregida                      |

---

### 4. `ERP_CUSTOMER`

**Acciones realizadas:**
- Validación de PK (`CID`) → sin duplicados ni nulos.  
- Estandarización de género:
  - `M` o `Male` → `Male`  
  - `F` o `Female` → `Female`  
  - Otros/nulos → `n/a`  
- Creación de `id_customer` a partir de los últimos 5 caracteres de `CID`.  

**Tabla final:** `DWH_SALES_DB.silver.ERP_CUSTOMER`

| Columna      | Descripción                         |
|--------------|-------------------------------------|
| CID          | Identificador original              |
| id_customer  | ID generado (últimos 5 caracteres)  |
| bird_date    | Fecha de nacimiento                 |
| Genre        | Género estandarizado                |

---

### 5. `ERP_LOCALIDAD`

**Acciones realizadas:**
- Estandarización de país (`CNTRY`) → mapeo a nombres completos.  
  - `DE` → `Germany`  
  - `USA` o `US` → `United States`  
  - Otros → país correspondiente o `n/a`.  
- Creación de `customer_id` desde `CID`.  

**Tabla final:** `DWH_SALES_DB.silver.ERP_LOCALIDAD`

| Columna      | Descripción                         |
|--------------|-------------------------------------|
| CID          | Identificador original              |
| customer_id  | ID derivado de `CID`                |
| Country      | País estandarizado                  |

---

### 6. `ERP_PRODUCTS`

**Acciones realizadas:**
- Validación de duplicados en `ID`.  
- Confirmación de no existencia de nulos en campos clave.  
- Creación de campo `id_table` con `ROW_NUMBER()` como PK técnica.  

**Tabla final:** `DWH_SALES_DB.silver.ERP_PRODUCTS`

| Columna      | Descripción                         |
|--------------|-------------------------------------|
| id_table     | PK generada                         |
| ID           | Identificador producto              |
| CAT          | Categoría                           |
| SUBCAT       | Subcategoría                        |
| MAINTENANCE  | Indicador mantenimiento             |

---

## Resumen de la Capa Silver

- Se **depuraron duplicados** en todas las tablas críticas.  
- Se **estandarizaron valores** de género, estado civil, países y líneas de productos.  
- Se corrigieron **fechas inválidas** y se imputaron valores nulos en ventas y precios.  
- Se generaron **nuevas claves** para asegurar integridad referencial entre CRM y ERP.  
- Todas las tablas limpias se cargaron en el esquema `DWH_SALES_DB.silver`.  

---

✍️ **Equipo de Datos**


GOLD

# Proyecto DWH_SALES_DB - Capa Gold

## Descripción

La **capa Gold** es la última etapa del pipeline de datos.  
Aquí los datos ya **limpios y estandarizados** en la capa **Silver** son organizados en vistas de tipo **dimensional (DIM)** y **de hechos (FACT)** para su análisis.  

Estas vistas son la base para construir reportes y tableros de **Power BI** u otras herramientas de BI.

---

## Objetivos de la Capa Gold

- Consolidar datos de clientes, productos y ventas.  
- Crear modelos **dimensionados** (esquema estrella).  
- Exponer información lista para consumo analítico.  
- Agregar métricas como edad, totales y fechas clave.  

---

## Vistas creadas

### 1. `gold.DIM_INFO_CRM_INFO_CUSTOMER`

Vista de clientes consolidada entre CRM y ERP.  

**Transformaciones aplicadas:**
- Consolidación de género (`gender` de CRM o `Genre` de ERP).  
- Cálculo de edad a partir de la fecha de nacimiento (`DATEDIFF`).  
- Inclusión de país desde `ERP_LOCALIDAD`.  

**Definición:**

| Columna         | Descripción                           |
|-----------------|---------------------------------------|
| customer_id     | Identificador único del cliente       |
| frist_name      | Nombre                                |
| last_name       | Apellido                              |
| Gender          | Género estandarizado                  |
| marital_status  | Estado civil                          |
| create_at       | Fecha de creación en CRM              |
| bird_date       | Fecha de nacimiento (ERP)             |
| Age             | Edad calculada                        |
| Country         | País estandarizado                    |

---

### 2. `gold.FACT_CRM_SALES_INFO`

Vista de hechos de ventas.  

**Transformaciones aplicadas:**
- Generación de PK artificial (`ROW_NUMBER()`).  
- Unificación de claves de producto (`PK_ID_SALES`, `PK_KEY_PRODUCT_INFO`).  
- Inclusión de fechas estandarizadas (orden, envío, vencimiento).  
- Corrección de precios y totales (`sls_sales_fomarted`, `sls_price_fomarted`).  

**Definición:**

| Columna           | Descripción                          |
|-------------------|--------------------------------------|
| ID                | Identificador de la transacción      |
| PK_ID_SALES       | Clave de producto en ventas          |
| PK_KEY_PRODUCT_INFO | Clave de producto en catálogo       |
| FK_CUSTOMER_ID    | Cliente asociado                     |
| created_at        | Fecha de orden                       |
| ship_date         | Fecha de envío                       |
| due_date          | Fecha de vencimiento                 |
| quantity          | Cantidad vendida                     |
| price_un          | Precio unitario                      |
| total             | Total de la transacción              |

---

### 3. `gold.DIM_CRM_INFO_PRODUCT`

Vista de productos integrados entre CRM y ERP.  

**Transformaciones aplicadas:**
- Creación de PK artificial (`ROW_NUMBER()`).  
- Unión entre productos de CRM y ERP (`crm_product_key_erp_product = ERP.ID`).  
- Inclusión de categorías, subcategorías y mantenimiento.  
- Normalización de fechas de inicio y fin de productos.  

**Definición:**

| Columna             | Descripción                         |
|----------------------|-------------------------------------|
| ID                  | Identificador de la vista           |
| product_id          | ID del producto en CRM              |
| PK_ERP_PRODUCT      | Llave asociada en ERP               |
| PK_SALES_INFO       | Llave asociada en ventas            |
| product_name        | Nombre del producto                 |
| product_cost        | Costo                               |
| product_line        | Línea estandarizada (ej. Mountain)  |
| categori            | Categoría del ERP                   |
| sub_categori        | Subcategoría del ERP                |
| product_start_date  | Fecha de inicio válida              |
| product_end_date    | Fecha de fin (si aplica)            |
| support_maintenance | Indicador de mantenimiento          |

---

## Modelo Estrella (Star Schema)

Con las vistas de la capa **Gold** se construye un esquema estrella compuesto por:  

- **Hechos (FACT):**  
  - `FACT_CRM_SALES_INFO` (ventas).  

- **Dimensiones (DIM):**  
  - `DIM_INFO_CRM_INFO_CUSTOMER` (clientes).  
  - `DIM_CRM_INFO_PRODUCT` (productos).  

Este modelo permite generar análisis en **Power BI** tales como:  
- Ventas por cliente, país o estado civil.  
- Ventas por línea de producto, categoría o subcategoría.  
- Evolución de ventas en el tiempo (órdenes, envíos, vencimientos).  
- Análisis demográfico (edad, género, localización).  

---

## Consultas de Validación

- Validar rango de fechas de ventas:

```sql
SELECT MIN(created_at), MAX(created_at) 
FROM gold.FACT_CRM_SALES_INFO;
```

Validar consistencia de llaves entre dimensiones y hechos.

Filtrar datos inválidos (ejemplo de fechas corruptas):


SELECT * 
FROM gold.FACT_CRM_SALES_INFO
WHERE created_at = '0//0';



