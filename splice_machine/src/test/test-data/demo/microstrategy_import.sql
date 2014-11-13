CREATE SCHEMA splice_demo;

CREATE TABLE splice_demo.category (
	cat_id			INT,
	cat_name		VARCHAR(128),
	cat_name_de		VARCHAR(128),
	cat_name_fr		VARCHAR(128),
	cat_name_es		VARCHAR(128),
	cat_name_it		VARCHAR(128),
	cat_name_po		VARCHAR(128),
	cat_name_ja		VARCHAR(128),
	cat_name_sch		VARCHAR(128),
	cat_name_ko		VARCHAR(128));

CREATE TABLE splice_demo.category_sub (
	sbc_id			INT,
	sbc_desc		VARCHAR(128),
	sbc_category_id 	INT,
	sbc_desc_de		VARCHAR(128),
	sbc_desc_fr		VARCHAR(128),
	sbc_desc_es		VARCHAR(128),
	sbc_desc_it		VARCHAR(128),
	sbc_desc_po		VARCHAR(128),
	sbc_desc_ja		VARCHAR(128),
	sbc_desc_sch		VARCHAR(128),
	sbc_desc_ko		VARCHAR(128));

CREATE TABLE splice_demo.customer (
	cst_id 			INT, 
	cst_last_name 		VARCHAR(64), 
	cst_first_name 		VARCHAR(64),
	cst_gender_id		INT, 
	cst_birthdate		TIMESTAMP,
	cst_email		VARCHAR(64),
	cst_address		VARCHAR(64),
	cst_zipcode		VARCHAR(16),
	cst_income_id		INT,
	cst_city_id		INT,
	cst_age_years		INT,
	cst_agerange_id		INT,
	cst_maritalstatus_id	INT,
	cst_education_id	INT,
	cst_housingtype_id	INT,
	cst_householdcount_id	INT,
	cst_plan_id		INT,
	cst_first_order		TIMESTAMP,
	cst_last_order		TIMESTAMP,
	cst_tenure		INT,
	cst_recency		INT,
	cst_status_id		INT);

CREATE TABLE splice_demo.item (
	itm_id			INT,
	itm_name		VARCHAR(128),
	itm_long_desc		VARCHAR(32672),
	itm_foreign_name	VARCHAR(128),
	itm_url			VARCHAR(1024),
	itm_disc_cd		VARCHAR(64),
	itm_upc			VARCHAR(64),
	itm_warranty		VARCHAR(1),
	itm_unit_price		FLOAT,
	itm_unit_cost		FLOAT,
	itm_subcat_id		INT,
	itm_supplier_id		INT,
	itm_brand_id		INT,
	itm_name_de		VARCHAR(128),
	itm_name_fr		VARCHAR(128),
	itm_name_es		VARCHAR(128),
	itm_name_it		VARCHAR(128),
	itm_name_po		VARCHAR(128),
	itm_name_ja		VARCHAR(128),
	itm_name_sch		VARCHAR(128),
	itm_name_ko		VARCHAR(128),
	itm_long_desc_de	VARCHAR(32672),
	itm_long_desc_fr	VARCHAR(32672),
	itm_long_desc_es	VARCHAR(32672),
	itm_long_desc_it	VARCHAR(32672),
	itm_long_desc_po	VARCHAR(32672),
	itm_itm_long_desc_ja	VARCHAR(32672),
	itm_long_desc_sch	VARCHAR(32672),
	itm_long_desc_ko	VARCHAR(32672));

CREATE TABLE splice_demo.order_line (
	orl_order_id 		VARCHAR(50), 
	orl_item_id 		INT, 
	orl_amt 		INT, 
	orl_date 		TIMESTAMP, 
	orl_emp_id 		INT, 
	orl_promotion_id 	INT, 
	orl_qty_sold 		INT, 
	orl_unit_price 		FLOAT, 
	orl_unit_cost 		FLOAT, 
	orl_discount 		FLOAT, 
	orl_customer_id 	INT);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('splice_demo', 'ORDER_LINE', null, null, '/Users/johnleach/ms/microstrategydata/order_line_500K.csv', ',', '"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('splice_demo', 'ITEM', null, null, '/Users/johnleach/ms/microstrategydata/item.csv', ',', '"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('splice_demo', 'CUSTOMER', null, null, '/Users/johnleach/ms/microstrategydata/customer_iso.csv', ',', '"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('splice_demo', 'CATEGORY', null, null, '/Users/johnleach/ms/microstrategydata/category.csv', ',', '"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('splice_demo', 'CATEGORY_SUB', null, null, '/Users/johnleach/ms/microstrategydata/category_sub.csv', ',', '"', null);

create view splice_demo.V_CATEGORY as select CAT_ID, UPPER(CAT_NAME) as CAT_NAME from Splice_demo.category;

create view splice_demo.V_CATEGORY_SUB as select sbc_id, UPPER(sbc_desc) as sbc_desc, sbc_category_id from Splice_demo.category_sub;

create view splice_demo.V_CUSTOMER as 
select cst_id, cst_last_name,cst_first_name,case 
when cst_gender_id = 1 then 'Male'
when cst_gender_id = 2 then 'Female'
else 'Unknown' end as cst_gender, cst_birthdate,cst_email,cst_address,cst_zipcode,
	cst_income_id,cst_city_id,cst_age_years,cst_agerange_id,cst_maritalstatus_id,cst_education_id,
	cst_housingtype_id,cst_householdcount_id,cst_plan_id,cst_first_order,cst_last_order,
	cst_tenure,cst_recency,cst_status_id from splice_demo.CUSTOMER;

	CREATE FUNCTION REPLACE2(str VARCHAR(50), matchStr VARCHAR(50), replaceStr VARCHAR(50)) RETURNS VARCHAR(50)
PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA
EXTERNAL NAME 'org.apache.commons.lang.StringUtils.replace';


org.apache.commons.lang.StringUtils
