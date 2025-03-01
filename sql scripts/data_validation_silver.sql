----------Data checks for prd_info table------------

---Data Validation Checks

--1. Duplicate check
select prd_id, count(*)
from bronze.crm_prd_info
group by 1,2,3,4,5,6,7
having count(*)>1 or prd_id is null

--2. distinct product line
select distinct prd_line, count(*)
from bronze.crm_prd_info
group by 1

--3. check for unwanted spaces
select prd_nm
from  bronze.crm_prd_info
where prd_nm != trim(prd_nm)

--4. Data quality of prd_cost
select distinct prd_cost, count(*)
from bronze.crm_prd_info
where prd_cost <0 or prd_cost is null
group by 1

--5. Some records prd_start_dt is > prd_end_dt.
select *, CAST(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) as DATE)-1 as new_prd_end_date
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R',
'AC-HE-HL-U509')