
---- customer table - Gold layer ---------

CREATE VIEW gold.dim_customers as 

select 
	row_number() over(order by a.cst_id) as customer_key,
	a.cst_id as customer_id,
	a.cst_key as customer_number,
	a.cst_firstname as first_name, 
	a.cst_lastname as last_name,
	c.cntry as country,
	a.cst_marital as marital_status,
	case 
		when a.cst_gndr != 'n/a' then a.cst_gndr
		else COALESCE(b.gen,'n/a')
	end as gender,
	b.bdate as birth_date,
	a.cst_create_date as create_date
from silver.crm_cust_info a left join silver.erp_cust_az12 b 
on a.cst_key = b.cid
left join silver.erp_loc_a101 c on a.cst_key = c.cid

---- Product table - Gold layer ---------


CREATE VIEW gold.dim_products as 
select 
	row_number() over(order by pd.prd_start_dt,pd.prd_key) as product_key,
	pd.prd_id as product_id, 
	pd.prd_key as product_number, 
	pd.prd_nm as product_name,
	pd.cat_id as category_id,
	ps.cat as category,
	ps.subcat as sub_category,
	ps.maintenance,
	pd.prd_cost as cost,
	pd.prd_line as product_line,
	pd.prd_start_dt as start_date
from silver.crm_prd_info pd left join silver.erp_px_cat_g1v2 ps 
on pd.cat_id = ps.id
where prd_end_dt is null

---- Sales table - Gold layer ---------

CREATE VIEW gold.fact_sales as
select 
	s.sls_ord_nm as order_number,
	c.customer_key,
	p.product_key,
	case
		when s.sls_order_dt is null then s.sls_ship_dt
		else s.sls_order_dt
	end as order_date,
	s.sls_ship_dt as shipping_date,
	s.sls_due_dt as due_date,
	s.sls_sales as sales_amount,
	s.sls_quantity as quantity,
	s.sls_price as price
from silver.crm_sales_details s 
left join gold.dim_products p
on s.sls_prd_key = p.product_number
left join gold.dim_customers c
on s.sls_cust_id = c.customer_id