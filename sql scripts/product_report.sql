/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/

--------------------------------------------------------------------
-----------------------Product Level Report-------------------------
--------------------------------------------------------------------


/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products
---------------------------------------------------------------------------*/

with base_query as (
select 
	s.order_number, 
	s.customer_key,
	s.order_date,
	s.sales_amount,
	s.quantity,
	s.price,
	p.product_id, 
	p.product_name,
	p.category,
	p.sub_category,
	p.cost
from gold.fact_sales s left join gold.dim_products p
on p.product_key = s.product_key),

/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/

product_aggregates as (
select 
product_name, 
category, 
sub_category, 
cost,
(EXTRACT(year from age(max(order_date), min(order_date))))*12 + EXTRACT(month from age(max(order_date), min(order_date))) as lifespan,
count(distinct order_number) as total_orders,
count(distinct customer_key) as total_unique_customers,
sum(sales_amount) as total_sales, 
sum(quantity) as total_quantity,
sum(price) as total_price
from base_query
group by 
	product_name, 
	category, 
	sub_category, 
	cost)

/*---------------------------------------------------------------------------
  3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/

select 
product_name,
category, 
sub_category,
case 
	when total_sales>50000 then 'High-Performer'
	when total_sales>=10000 then 'Mid-Range'
	else 'Low-Performer'
end as product_segment,
cost, 
lifespan,
total_unique_customers, 
total_orders,
total_sales,
total_quantity,
case 
	when total_orders = 0 then 0
	else ROUND(total_sales/total_orders,2)
end as average_order_value,
case 
	when lifespan = 0 then 0
	else ROUND(total_sales/lifespan,2)
end as average_monthly_revenue
from product_aggregates



