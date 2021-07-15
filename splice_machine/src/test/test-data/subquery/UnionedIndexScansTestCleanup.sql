DROP TABLE T1;
DROP TABLE T2;
call syscs_util.syscs_set_global_database_property('splice.optimizer.favorUnionedIndexScans', null);
CALL SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE();
