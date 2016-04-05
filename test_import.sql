
call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'LINEITEM', null, '/Users/scottfines/workspace/test/performance/tpch/tpch1g/data/TPCH1g/lineitem', '|', null, null, null, null, 0, '/Users/scottfines/workspace/engine/hbase_sql/target/BAD', true, null);
select count(*) from tpch.lineitem;
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'ORDERS',   null, '/TPCH/1/orders',   '|', null, null, null, null, 0, '/BAD', true, null)
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'CUSTOMER', null, '/TPCH/1/customer', '|', null, null, null, null, 0, '/BAD', true, null)
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'PARTSUPP', null, '/TPCH/1/partsupp', '|', null, null, null, null, 0, '/BAD', true, null)
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'SUPPLIER', null, '/TPCH/1/supplier', '|', null, null, null, null, 0, '/BAD', true, null)
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'PART',     null, '/TPCH/1/part',     '|', null, null, null, null, 0, '/BAD', true, null)
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'REGION',   null, '/TPCH/1/region',   '|', null, null, null, null, 0, '/BAD', true, null)
--call SYSCS_UTIL.IMPORT_DATA ('TPCH', 'NATION',   null, '/TPCH/1/nation',   '|', null, null, null, null, 0, '/BAD', true, null)

