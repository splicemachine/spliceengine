create table t1_like(a varchar(30));
create table t2_like(a varchar(30));

insert into t1_like values ('a_'), ('a'), ('a ');
insert into t2_like values ('Steve Raster'), ('Steve Mossely'), ('Stephen Tuvesco');

create table t1(a dec(31,30), b date, c char(30), d time);

insert into t1 values (0.012345678901234567890123456789, DATE('1988-12-25'), '3', TIME( {ts'2001-01-01 12:00:00.123456'} )), (-.3, DATE('1990-4-25'), '5', TIME( {ts'2001-01-01 12:00:00.123456'}  )), (1.0, DATE('0001-12-25'), '-11', TIME( {ts'2001-01-01 12:00:00.123456'}  ));

create table t2(a double, c varchar(30));
insert into t2 values (11.1, '2001-01-01 12:00:00');

create table t3 (a int, b int, c int, d int, e int);
insert into t3 values (null, null, null, null, 1);
insert into t3 values (null,null,null,3,4);

create table t3_case (a int, b int, c int, d int, e int);
insert into t3_case values (null, null, null, null, 1);
insert into t3_case values (1,1,2,2,3);
insert into t3_case values (1,2,1,2,3);
insert into t3_case values (1,2,2,1,3);

create table t4 (a varchar(40));
insert into t4 values ('tHis iS a tEST.');

create table t5 (a clob);
insert into t5 values ('abcdefghij');

create table t6 (a date, b date, c date);
insert into t6 values (date('2001-01-01'), date('2001-01-01'), date('2001-01-02'));

create table t7 (a timestamp);
insert into t7 values ({ts'2001-01-01 12:00:00'});

create table ts_int (t tinyint, s smallint, i int, l bigint);
insert into ts_int values
                        (127, 32767, 2147483647, 9223372036854775807),
                        (127, 32767, 2147483647, 9223372036854775807),
                        (127, 32767, 2147483647, 9223372036854775807),
                        (127, 32767, 2147483647, 9223372036854775807),
                        (null, null, null, null),
                        (null, null, null, null),
                        (null, null, null, null);
create index ix_int on ts_int(l, i);

create table ts_decimal (a dec(11,1), b int);
insert into ts_decimal select * from (values
                        (9999999999.7, 1),
                        (9999999999.9, 1),
                        (9999999999.9, 1)) dt order by 1;

create table ts_double (a double, b double, c int);
insert into ts_double values (1.79769E+308, 1.49769E+308, 1);

create table t1_trunc(a timestamp);
insert into t1_trunc values( {ts '2000-06-07 17:12:30.0'});

create table t2_trunc(a decimal(10,4));
insert into t2_trunc values (23.123), (24.29), (24.55), (25.55), (26.9999);

create table t8 (a bigint);
insert into t8 values(9223372036854775807);



