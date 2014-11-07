-- $ID$
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

create view revenue0 (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
                l_shipdate >= date('1996-01-01')
                and l_shipdate < date({fn TIMESTAMPADD(SQL_TSI_MONTH, 3, cast('1996-01-01 00:00:00' as timestamp))})
	group by
		l_suppkey