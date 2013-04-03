CREATE TABLE category (
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
call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'CATEGORY', null, null, '/home/ryan/splice/spliceengine/structured_derby/src/test/resources/small_msdatasample/category.csv', ',', '"', null);

CREATE TABLE category_sub (
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
call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'CATEGORY_SUB', null, null, '/home/ryan/splice/spliceengine/structured_derby/src/test/resources/small_msdatasample/category_sub.csv', ',', '"', null);

CREATE TABLE customer (
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
call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'CUSTOMER', null, null, '/home/ryan/splice/spliceengine/structured_derby/src/test/resources/small_msdatasample/customer_iso.csv', ',', '"', null);


CREATE TABLE item (
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
call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'ITEM', null, null, '/home/ryan/splice/spliceengine/structured_derby/src/test/resources/small_msdatasample/item.csv', ',', '"', null);


CREATE TABLE order_header (
	orh_id			VARCHAR(50), 
	orh_date 		TIMESTAMP, 
	orh_emp_id		INT,
	orh_amt			FLOAT, 
	orh_cost		FLOAT, 
	orh_qty			FLOAT, 
	orh_ship_date		TIMESTAMP,
	orh_rush		INT);
call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'ORDER_HEADER', null, null, '/home/ryan/splice/spliceengine/structured_derby/src/test/resources/small_msdatasample/order_header.csv', ',', '"', null);

CREATE TABLE order_line (
	orl_order_id 		VARCHAR(50), 
	orl_amt 		INT,
	orl_item_id 		INT,
	orl_date 		TIMESTAMP,
	orl_emp_id 		INT,
	orl_promotion_id 	INT, 
	orl_qty_sold 		INT, 
	orl_unit_price 		FLOAT, 
	orl_unit_cost 		FLOAT, 
	orl_discount 		FLOAT, 
	orl_customer_id 	INT);
call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'ORDER_LINE', null, null, '/home/ryan/splice/spliceengine/structured_derby/src/test/resources/small_msdatasample/order_line.csv', ',', '"', null);