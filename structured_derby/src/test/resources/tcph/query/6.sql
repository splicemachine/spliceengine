-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
set schema tpch1x;
VALUES (CURRENT_TIMESTAMP) ;
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
        l_shipdate >= date('1994-01-01')
        and l_shipdate < date({fn TIMESTAMPADD(SQL_TSI_YEAR, -1, cast('1994-01-01 00:00:00' as timestamp))})
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24;
VALUES (CURRENT_TIMESTAMP) ;
