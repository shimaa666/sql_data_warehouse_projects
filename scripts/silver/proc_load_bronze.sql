/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
create or alter procedure silver.load_silver as
begin


 DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();


	PRINT '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver_crm_cust_info;;
	PRINT '>> Inserting Data Into: silver.crm_cust_info';

	INSERT INTO silver_crm_cust_info (
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
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,

		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'n/a'
		END AS cst_marital_status,

		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'n/a'
		END AS cst_gndr,

		cst_create_date

	FROM (
		SELECT *,
			   ROW_NUMBER() OVER (
				   PARTITION BY cst_id 
				   ORDER BY cst_create_date DESC
			   ) AS flag_last
		FROM bronze_crm_cust_info
	) t
	WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



    -- Loading silver.crm_prd_info
   SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver_crm_pre_info';
	TRUNCATE TABLE silver_crm_pre_info;
	PRINT '>> Inserting Data Into: silver_crm_pre_info';
	insert into silver_crm_pre_info(
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
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			WHEN 'M' THEN 'Mountain'
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		cast(prd_start_dt as date)as prd_start_dt,
		cast(LEAD(prd_start_dt)OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as date) AS prd_end_dt
	FROM bronze_crm_pre_info;

      SET @end_time = GETDATE();
      PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
      PRINT '>> -------------';

	PRINT '>> Truncating Table: silver_crm_Sales_details';
	TRUNCATE TABLE silver_crm_Sales_details;
	PRINT '>> Inserting Data Into: silver_crm_Sales_details';

    -- Loading crm_sales_details
    SET @start_time = GETDATE();
	INSERT INTO silver_crm_Sales_details
	(
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

		CASE 
			WHEN sls_order_dt = 0 
				 OR LEN(CAST(sls_order_dt AS VARCHAR(20))) != 8 
			THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR(20)) AS DATE)
		END,

		CASE 
			WHEN sls_ship_dt = 0 
				 OR LEN(CAST(sls_ship_dt AS VARCHAR(20))) != 8 
			THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR(20)) AS DATE)
		END,

		CASE 
			WHEN sls_due_dt = 0 
				 OR LEN(CAST(sls_due_dt AS VARCHAR(20))) != 8 
			THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR(20)) AS DATE)
		END,

		CASE 
			WHEN sls_sales IS NULL 
				 OR sls_sales <= 0 
				 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END,

		sls_quantity,

		CASE 
			WHEN sls_price IS NULL 
				 OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END

	FROM bronze_crm_Sales_details;
	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';

    -- Loading erp_cust_az12
    SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver_erp_cust_az12';
	TRUNCATE TABLE silver_erp_cust_az12;
	PRINT '>> Inserting Data Into: silver_erp_cust_az12';


	insert into  silver_erp_cust_az12(
	CID ,
	BDATE ,
	GEN 
	)

	SELECT
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
	ELSE cid
	END AS cid,

	CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
	END AS bdate,

	CASE WHEN UPPER (TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' WHEN UPPER (TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	ELSE 'n/a'

	END AS gen

	FROM bronze_erp_cust_az12

	SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';


	
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

   -- Loading erp_loc_a101
    SET @start_time = GETDATE();
	PRINT '>> Truncating Table: silver_erp_loc_a101';
	TRUNCATE TABLE silver_erp_loc_a101;
	PRINT '>> Inserting Data Into: silver_erp_loc_a101';


	INSERT INTO silver_erp_loc_a101
	(
	cid, cntry
	)

	SELECT
	REPLACE(cid, '-', '') cid,

	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'

	WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'

	WHEN TRIM(cntry) =''OR cntry IS NULL THEN 'n/a'

	ELSE TRIM(cntry)

	END AS cntry
	FROM bronze_erp_loc_a101

    SET @end_time = GETDATE();
    PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';


	-- Loading erp_px_cat_g1v2
	SET @start_time = GETDATE();

	PRINT '>> Truncating Table: silver_erp_px_cat_g1v2';
	TRUNCATE TABLE silver_erp_px_cat_g1v2;
	PRINT '>> Inserting Data Into: silver_erp_px_cat_g1v2';

	INSERT INTO silver_erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	SELECT
	id,
	cat,
	subcat,
	maintenance
	FROM bronze_erp_px_cat_g1v2

	SET @end_time = GETDATE();
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
    PRINT '>> -------------';


	SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH

end
