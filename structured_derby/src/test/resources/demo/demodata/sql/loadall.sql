CREATE TABLE T_HEADER
(
	TRANSACTION_HEADER_KEY BIGINT NOT NULL, 
	CUSTOMER_MASTER_ID BIGINT, 
	TRANSACTION_DT DATE NOT NULL, 
	STORE_NBR SMALLINT NOT NULL, 
	EXCHANGE_RATE_PERCENT DOUBLE PRECISION, 
	GEOCAPTURE_FLG VARCHAR(1), 
    PRIMARY KEY(TRANSACTION_HEADER_KEY)
);

CREATE TABLE T_DETAIL
(	
	TRANSACTION_HEADER_KEY BIGINT NOT NULL,
	TRANSACTION_DETAIL_KEY BIGINT NOT NULL, 
	CUSTOMER_MASTER_ID BIGINT, 
	TRANSACTION_DT DATE NOT NULL, 
	ORIGINAL_SKU_CATEGORY_ID INTEGER, 
    PRIMARY KEY(TRANSACTION_HEADER_KEY, TRANSACTION_DETAIL_KEY)
);

CREATE TABLE CUSTOMERS
(
	CUSTOMER_MASTER_ID BIGINT PRIMARY KEY
);


call SYSCS_UTIL.IMPORT_DATA('APP','THEADER',null,'demodata/data/theader.csv.gz',',',null,null,null,null,0,null);
call SYSCS_UTIL.IMPORT_DATA('APP','TDETAIL',null,'demodata/data/tdetail.csv.gz',',',null,null,null,null,0,null);
call SYSCS_UTIL.IMPORT_DATA('APP','TGT',null,'demodata/data/customers.csv.gz',',',null,null,null,null,0,null);

create index tdidx1 on t_detail(original_sku_category_id,transaction_dt,customer_master_id);
create index thidx2 on t_header(customer_master_id, transaction_dt);
