drop table if exists t;

create table t (a int, b varchar(20), PRIMARY KEY(b));
create index t_idx on t(b desc);

insert into t (a,b) values (1,'hello');
select * from t --SPLICE-PROPERTIES index=T_IDX
where b = 'hello';
