CALL SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE();
drop table if exists t1;
drop table if exists t2;
create table t1 (a int, b int, c int, primary key(b,a));
create table t2 (a int, b int);
create index idx1 on t1(a);
create index idx3 on t1(c);
create index idx2 on t2(a, b);
create index idx4 on t2(b);

insert into t1 values (1,1,1);
insert into t1 select a+1,1,c+1 from t1;
insert into t1 select a+2,1,c+2 from t1;
insert into t1 select a+4,1,c+4 from t1;

insert into t2 select a,b from t1 where a <= 4;
insert into t2 select a,b+2 from t2;
insert into t2 select a,b+4 from t2;

insert into t1 select a, b+1,c from t1;
insert into t1 select a, b+2,c from t1;

analyze table t1;
analyze table t2;


