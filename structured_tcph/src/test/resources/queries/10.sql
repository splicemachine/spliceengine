-- $ID$
-- TPC-H/TPC-R Returned Item Reporting Query (Q10)
-- Functional Query Definition
-- Approved February 1998
set schema tpch1x;
VALUES (CURRENT_TIMESTAMP);
select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
        and o_orderdate >= date('1993-10-01')
        and o_orderdate < date({fn TIMESTAMPADD(SQL_TSI_MONTH, 3, cast('1993-10-01 00:00:00' as timestamp))})
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc;
VALUES (CURRENT_TIMESTAMP);
