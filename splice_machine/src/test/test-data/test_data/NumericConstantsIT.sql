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

create table table_real (
  a real not null,
  primary key(a)
);

create table table_double (
  a double not null,
  primary key(a)
);

create table table_decimal_5_0 (
  a decimal not null,
  primary key(a)
);

CREATE TABLE table_decimal_11_2 (
  a DECIMAL(11, 2) NOT NULL,
  PRIMARY KEY (a)
);

CREATE TABLE table_decimal_38_0 (
  a DECIMAL(38, 0) NOT NULL,
  PRIMARY KEY (a)
);

CREATE TABLE table_decimal_38_38 (
  a DECIMAL(38, 38) NOT NULL,
  PRIMARY KEY (a)
);

insert into table_smallint values (-32768),(-1), (0), (1), (32767);
insert into table_integer values (-2147483648),(-1), (0), (1), (2147483647);
insert into table_bigint values (-9223372036854775808),(-1), (0), (1), (9223372036854775807);

insert into table_real values (-3.402E+38),(-1.0), (0.0), (1.0), (3.402E+38);
insert into table_double values (-1.79769E+308),(-1.0), (0.0), (1.0), (1.79769E+308);
insert into table_decimal_5_0 values (-99999.00),(-1.0), (0.0), (1.0), (99999.00);
insert into table_decimal_11_2 values (-999999999.99),(-1.0), (0.0), (1.0), (999999999.99);
insert into table_decimal_38_0 values (-99999999999999999999999999999999999999),(-1), (0), (1), (99999999999999999999999999999999999999);
insert into table_decimal_38_38 values (-.99999999999999999999999999999999999999),(-.1), (0), (.1), (.99999999999999999999999999999999999999);