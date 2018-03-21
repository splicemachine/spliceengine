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

-- replace yourPath with the appropriate full path to the demodata directory (i.e. '/home/user/splicemachine/demodata/data/theader.csv')

call SYSCS_UTIL.IMPORT_DATA('SPLICE' , 'T_HEADER'  , null , '<yourPath>/demodata/data/theader.csv'   , ',' , null , null , 'yyyy-MM-dd' , null , 0 , null , null , null);
call SYSCS_UTIL.IMPORT_DATA('SPLICE' , 'T_DETAIL'  , null , '<yourPath>/demodata/data/tdetail.csv'   , ',' , null , null , 'yyyy-MM-dd' , null , 0 , null , null , null);
call SYSCS_UTIL.IMPORT_DATA('SPLICE' , 'CUSTOMERS' , null , '<yourPath>/demodata/data/customers.csv' , ',' , null , null , null         , null , 0 , null , null , null);


create index tdidx1 on t_detail(original_sku_category_id,transaction_dt,customer_master_id);
create index thidx2 on t_header(customer_master_id, transaction_dt);
