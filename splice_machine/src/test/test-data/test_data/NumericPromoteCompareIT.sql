-- ================================================
-- Test schema and data for NumericPromoteCompareIT
-- ================================================

create table customer (cust_id_int int, cust_id_sml smallint, cust_id_dec decimal(4, 2), cust_id_num numeric(10, 2));

insert into customer(cust_id_int, cust_id_sml, cust_id_dec, cust_id_num) values (1, 1, 1.00, 1.00);
insert into customer(cust_id_int, cust_id_sml, cust_id_dec, cust_id_num) values (2, 2, 2.00, 2.00);
insert into customer(cust_id_int, cust_id_sml, cust_id_dec, cust_id_num) values (3, 3, 3.00, 3.00);
insert into customer(cust_id_int, cust_id_sml, cust_id_dec, cust_id_num) values (4, 4, 4.00, 4.00);
insert into customer(cust_id_int, cust_id_sml, cust_id_dec, cust_id_num) values (5, 5, 5.01, 5.01);
