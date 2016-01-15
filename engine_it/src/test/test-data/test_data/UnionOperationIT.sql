create table ST_MARS (
  empId int,
  empNo int,
  name varchar(40)
);

create table ST_EARTH (
  empId int,
  empNo int,
  name varchar(40)
);

create table T1 (
  i int,
  s smallint,
  d double precision,
  r real,
  c10 char(10),
  c30 char(30),
  vc10 varchar(10),
  vc30 varchar(30)
);

create table T2 (
  i int,
  s smallint,
  d double precision,
  r real,
  c10 char(10),
  c30 char(30),
  vc10 varchar(10),
  vc30 varchar(30)
);

create table DUPS (
  i int,
  s smallint,
  d double precision,
  r real,
  c10 char(10),
  c30 char(30),
  vc10 varchar(10),
  vc30 varchar(30)
);

create table empty_table_1 (col1 int, col2 int, col3 varchar(20));
create table empty_table_2 (col1 int, col2 int, col3 varchar(20));
create table empty_table_3 (col1 int, col2 int, col3 varchar(20));
create table empty_table_4 (col1 int, col2 int, col3 varchar(20));

insert into ST_MARS values (3, 1, 'Nimoy-Leonard');
insert into ST_MARS values (4, 1, 'Patrick');
insert into ST_MARS values (5, 1,  NULL);
insert into ST_MARS values (6, 1, 'Mulgrew-Kate');
insert into ST_MARS values (7, 1, 'Shatner-William');

insert into ST_EARTH values (3, 1, 'Nimoy-Leonard');
insert into ST_EARTH values (4, 1, 'Ryan-Jeri');
insert into ST_EARTH values (5, 1, NULL);
insert into ST_EARTH values (6, 1, 'Spiner-Brent');
insert into ST_EARTH values (7, 1, 'Duncan-Robert');

insert into T1 values (1, 1, 1e1, 1e1, '11111', '11111     11', '11111','11111      11');
insert into T1 values (2, 2, 2e1, 2e1, '22222', '22222     22', '22222','22222      22');

insert into T2 values (null, null, null, null, null, null, null, null);
insert into T2 values (null, null, null, null, null, null, null, null);
insert into T2 values (3, 3, 3e1, 3e1, '33333', '33333     33', '33333','33333      33');
insert into T2 values (4, 4, 4e1, 4e1, '44444', '44444     44', '44444','44444      44');

insert into DUPS select * from T1 union all select * from T1;
