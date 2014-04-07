-- $ID$
-- TPC-H/TPC-R Potential Part Promotion Query (Q20)
-- Function Query Definition
-- Approved February 1998
set schema tpch1x;
VALUES (CURRENT_TIMESTAMP);
select s_name,s_address from supplier,nation
where s_suppkey in (
		select ps_suppkey from partsupp
		where ps_partkey in (select p_partkey from part where p_name like 'forest%')
			and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey 
										and l_suppkey = ps_suppkey
                                        and l_shipdate >= date('1994-01-01')
                                        and l_shipdate < date({fn TIMESTAMPADD(SQL_TSI_YEAR, 1, cast('1994-01-01 00:00:00' as timestamp))})
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA'
order by
	s_name;
VALUES (CURRENT_TIMESTAMP);
