-- ================================================
-- Test schema and data for NumericConstantsIT
--  ================================================

create table table_smallint (
  a smallint not null,
  primary key(a)
);

create table table_integer (
  a integer not null,
  primary key(a)
);

create table table_bigint (
  a bigint not null,
  primary key(a)
);


insert into table_smallint values (-32768),(-1), (0), (1), (32767);
insert into table_integer values (-2147483648),(-1), (0), (1), (2147483647);
insert into table_bigint values (-9223372036854775808),(-1), (0), (1), (9223372036854775807);

