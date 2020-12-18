SELECT COUNT (*) FROM SYS.SYSTABLES --splice-properties useSpark=true
;
create table t (a int);
insert into t values 1;
select * from t --splice-properties useSpark=true
;
drop table t;
