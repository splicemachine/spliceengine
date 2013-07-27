-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
set schema tpch1x;
VALUES (CURRENT_TIMESTAMP) ;
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
        o_orderdate >= date('1993-07-01')
        and o_orderdate < date({fn TIMESTAMPADD(SQL_TSI_MONTH, 3, cast('1993-07-01 00:00:00' as timestamp))})
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
VALUES (CURRENT_TIMESTAMP) ;
