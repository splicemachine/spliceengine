-- prereq: demo data loaded (import_splice.sql)
select cst_zipcode, sum(orl_qty_sold*orl_unit_price) from customer left outer join order_line on orl_customer_id=cst_id group by cst_zipcode;
