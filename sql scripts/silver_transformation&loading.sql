-- Issues observed in customer table
-- 1. Extra spaces in Name columns (first, last)
-- 2. Changing abbrevations to full names (Gender, Marital Status)
-- 3. Duplicates in the data at cst_id level (primary key) ---> Latest record is the updated one
CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
BEGIN 
	TRUNCATE TABLE silver.crm_cust_info;
	
	INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital,
		cst_gndr,
		cst_create_date
	)
	select cst_id, cst_key,
	TRIM(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case when upper(trim(cst_marital)) like 'S' then 'Single'
		 when upper(trim(cst_marital)) like 'M' then 'Married'
		 ELSE 'n/a'
	END as cst_marital,
	case when upper(trim(cst_gndr)) like 'M' then 'Male'
		 when upper(trim(cst_gndr)) like 'F' then 'Female'
		 ELSE 'n/a'
	END as cst_gndr,
	cst_create_date
	from 
	(select *, 
	row_number() over(partition by cst_id order by cst_create_date desc) as r_no
	from bronze.crm_cust_info
	where cst_id IS NOT NULL) a 
	where r_no =1;
	
	
	--------transformations on prd_info table-------------
	TRUNCATE TABLE silver.crm_prd_info;
	
	INSERT INTO silver.crm_prd_info
	(
	prd_id, 
	cat_id, 
	prd_key, 
	prd_nm, 
	prd_cost, 
	prd_line, 
	prd_start_dt,
	prd_end_dt
	)
	
	select prd_id, REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
	SUBSTRING(prd_key,7,length(prd_key)) as prd_key, 
	trim(prd_nm) as prd_nm,
	coalesce(prd_cost,0) as pd_cost,
	case UPPER(TRIM(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
	end as prd_line,
	CAST(prd_start_dt as DATE),
	CAST(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) as DATE)-1 as prd_end_dt
	from bronze.crm_prd_info
	;
	--------transformations on sales_details table-------------
	-- Handled invalid and missing data and data type in ord_dt, ship_dt columns
	-- Handled invalid and missing data and calculation issues in price, quantity and sales columns
	------------
	TRUNCATE TABLE silver.crm_sales_details;
	
	INSERT INTO silver.crm_sales_details
	(
		sls_ord_nm,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
	)
	
	select sls_ord_nm,sls_prd_key,sls_cust_id,
	case 
		when sls_order_dt=0 or length(CAST(sls_order_dt as varchar)) != 8 then NULL
		else CAST(CAST(sls_order_dt as varchar)AS DATE)
	END AS sls_order_dt,
	case 
		when sls_ship_dt=0 or length(CAST(sls_ship_dt as varchar)) != 8 then NULL
		else CAST(CAST(sls_ship_dt as varchar)AS DATE)
	END AS sls_ship_dt,
	case 
		when sls_due_dt=0 or length(CAST(sls_due_dt as varchar)) != 8 then NULL
		else CAST(CAST(sls_due_dt as varchar)AS DATE)
	END AS sls_due_dt,
	case 
		when sls_sales <=0 or sls_sales is null or sls_sales != (sls_quantity* abs(sls_price)) then sls_quantity*abs(sls_price)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case 
		when sls_price <=0 or sls_price is null then (sls_sales)/coalesce(sls_quantity,0)
		else sls_price
	end as sls_price
	from bronze.crm_sales_details
	;
	--------transformations on erp_cust_az12 table-------------
	
	TRUNCATE TABLE silver.erp_cust_az12;
	
	INSERT INTO silver.erp_cust_az12
	(cid,
	bdate,
	gen)
	
	select 
	 case 
	 	when cid like 'NAS%' then substring(cid,4, length(cid)) 
	 	else cid
	 end as cid,
	 case 
	 	when bdate> now() then NULL
		else bdate
	end as bdate,
	 case 
	 	when UPPER(TRIM(gen)) in ('M','MALE') then 'Male'
		when UPPER(TRIM(gen)) in ('F','FEMALE') then 'Female'
		else 'n/a'
	END AS gen
	from bronze.erp_cust_az12;
	
	--------transformations on erp_loc_a101 table-------------
	TRUNCATE TABLE silver.erp_loc_a101;
	
	INSERT INTO silver.erp_loc_a101
	(cid,cntry)
	
	select REPLACE(cid,'-','') as cid,
	case 
		 when TRIM(cntry) is null or trim(cntry) like '' then 'n/a'
		 when TRIM(cntry)  in ('US','USA','United States') then 'United States'
		 when TRIM(cntry)  in ('DE','Germany') then 'Germany'
	 else TRIM(cntry) 
	end as cntry
	from bronze.erp_loc_a101
	;
	--------transformations on erp_px_cat_g1v2 table-------------
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
	INSERT INTO  silver.erp_px_cat_g1v2 
	(id,cat,subcat,maintenance)
	
	select * from bronze.erp_px_cat_g1v2 ;
end
$$

call silver.load_silver()