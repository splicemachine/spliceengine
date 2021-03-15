
create table t1  (a1 int,
                  b1 int,
                  c1 int,
                  d1 int,
                  e1 int,
                  f1 int,
                  g1 int,
                  h1 timestamp, primary key(a1, b1, c1, d1, e1, f1, g1, h1));

create table t11 (a1 bigint,
                 b1 decimal(31,0),
                 c1 char(5),
                 d1 int,
                 e1 smallint,
                 f1 bigint,
                 g1 bigint,
                 h1 timestamp, primary key(a1, b1, c1, d1, e1, f1, g1, h1));

insert into t1 values(1,1,1,1,1,1,1, timestamp('2018-12-31 15:59:59.3211111'));
insert into t1 values(1,1,1,1,1,1,1, timestamp('1969-12-31 15:59:59.3211111'));
insert into t1 values(1,1,1,1,1,1,1, timestamp('1969-12-31 15:59:59.9999999'));
insert into t1 values(1,1,1,1,1,1,1, timestamp('1969-12-31 15:59:59.000001'));
insert into t1 values(2,1,1,1,1,1,1, timestamp('1969-12-31 15:59:59.001'));
insert into t1 values(2,1,1,1,1,1,1, timestamp('1969-12-31 15:59:59.00'));
insert into t1 values(2,1,1,1,1,1,1, timestamp('1970-12-31 15:59:59.00'));
insert into t1 values(2,1,1,1,1,1,1, timestamp('1999-12-31 15:59:59.00'));
insert into t1 values(2,1,1,1,1,1,1, timestamp('1995-01-01 15:59:59.00'));

insert into t1 select a1,b1+2,c1,d1+2,e1,f1+2,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1,b1+4,c1,d1+4,e1,f1+4,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1,b1+8,c1,d1+8,e1,f1+8,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1,b1+16,c1,d1+16,e1,f1+16,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;

insert into t1 select a1,b1+16,c1,d1+16,e1+2,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1,b1+16,c1,d1+16,e1+4,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1,b1+16,c1,d1+16,e1+8,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1,b1+16,c1,d1+16,e1+16,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;

insert into t1 select a1+2,b1,c1+2,d1,e1,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1+4,b1,c1+4,d1,e1,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1+8,b1,c1+8,d1,e1,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 1, h1) from t1;
insert into t1 select a1+16,b1,c1+16,d1,e1,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 2, h1) from t1;
insert into t1 select a1+32,b1,c1+32,d1,e1,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 2, h1) from t1;
insert into t1 select a1+64,b1,c1+64,d1,e1,f1,g1, TIMESTAMPADD(SQL_TSI_MINUTE, 2, h1) from t1;

insert into t1 values(-1,-1,-1,-1,-1,-1,-1, timestamp('2018-12-31 15:59:59.3211111'));


insert into t11 select * from t1;

create index t11_ix1 on t11(c1 DESC, d1 DESC);
create index t11_ix2 on t11(d1 DESC);
create index t11_ix3 on t11((a1 + 1) DESC, b1 + 1);
create index t11_ix4 on t11(a1, e1, c1);
create index t11_ix5 on t11(g1 + 1, (d1 + 1) DESC);

create index t1_ix3 on t1(a1 + 1, (b1 + 1) DESC);

analyze table t1;
analyze table t11;


