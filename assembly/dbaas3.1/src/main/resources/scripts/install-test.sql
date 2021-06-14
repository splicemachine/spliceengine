maximumdisplaywidth 40;
call SYSCS_UTIL.SYSCS_GET_VERSION_INFO();
call SYSCS_UTIL.SYSCS_GET_ALL_PROPERTIES();
select * from sys.systables;
select * from sys.systables order by tablename;
select t.tablename, c.conglomeratenumber from sys.systables t, sys.sysconglomerates c where t.tableid = c.tableid;
select count(*) from sys.systables;
