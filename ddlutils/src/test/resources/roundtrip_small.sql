
-- ----------------------------------------------------------------------- 
--  SCHEMA ROUNDTRIP_SMALL 
-- ----------------------------------------------------------------------- 

CREATE SCHEMA ROUNDTRIP_SMALL AUTHORIZATION JOJO;

SET SCHEMA ROUNDTRIP_SMALL;

-- ----------------------------------------------------------------------- 
-- TABLE ROUNDTRIP_SMALL.CATEGORY 
-- ----------------------------------------------------------------------- 

CREATE TABLE ROUNDTRIP_SMALL.CATEGORY
(
    CAT_ID INTEGER,
    CAT_NAME VARCHAR(128),
    CAT_NAME_DE VARCHAR(128),
    CAT_NAME_FR VARCHAR(128),
    CAT_NAME_ES VARCHAR(128),
    CAT_NAME_IT VARCHAR(128),
    CAT_NAME_PO VARCHAR(128),
    CAT_NAME_JA VARCHAR(128),
    CAT_NAME_SCH VARCHAR(128),
    CAT_NAME_KO VARCHAR(128)
);
call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_SMALL', 'CATEGORY', null, '<USER.DIR>/src/test/resources/small_msdatasample/category.csv', ',', '"', null,null,null,0,null,true,null);

-- ----------------------------------------------------------------------- 
-- TABLE ROUNDTRIP_SMALL.CATEGORY_SUB 
-- ----------------------------------------------------------------------- 

CREATE TABLE ROUNDTRIP_SMALL.CATEGORY_SUB
(
    SBC_ID INTEGER,
    SBC_DESC VARCHAR(128),
    SBC_CATEGORY_ID INTEGER,
    SBC_DESC_DE VARCHAR(128),
    SBC_DESC_FR VARCHAR(128),
    SBC_DESC_ES VARCHAR(128),
    SBC_DESC_IT VARCHAR(128),
    SBC_DESC_PO VARCHAR(128),
    SBC_DESC_JA VARCHAR(128),
    SBC_DESC_SCH VARCHAR(128),
    SBC_DESC_KO VARCHAR(128)
);
call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_SMALL', 'CATEGORY_SUB', null, '<USER.DIR>/src/test/resources/small_msdatasample/category_sub.csv', ',', '"', null,null,null,0,null,true,null);

-- ----------------------------------------------------------------------- 
-- TABLE ROUNDTRIP_SMALL.CUSTOMER 
-- ----------------------------------------------------------------------- 

CREATE TABLE ROUNDTRIP_SMALL.CUSTOMER
(
    CST_ID INTEGER,
    CST_LAST_NAME VARCHAR(64),
    CST_FIRST_NAME VARCHAR(64),
    CST_GENDER_ID INTEGER,
    CST_BIRTHDATE TIMESTAMP,
    CST_EMAIL VARCHAR(64),
    CST_ADDRESS VARCHAR(64),
    CST_ZIPCODE VARCHAR(16),
    CST_INCOME_ID INTEGER,
    CST_CITY_ID INTEGER,
    CST_AGE_YEARS INTEGER,
    CST_AGERANGE_ID INTEGER,
    CST_MARITALSTATUS_ID INTEGER,
    CST_EDUCATION_ID INTEGER,
    CST_HOUSINGTYPE_ID INTEGER,
    CST_HOUSEHOLDCOUNT_ID INTEGER,
    CST_PLAN_ID INTEGER,
    CST_FIRST_ORDER TIMESTAMP,
    CST_LAST_ORDER TIMESTAMP,
    CST_TENURE INTEGER,
    CST_RECENCY INTEGER,
    CST_STATUS_ID INTEGER
);
 call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_SMALL', 'CUSTOMER', null, '<USER.DIR>/src/test/resources/small_msdatasample/customer_iso.csv', ',', '"', null,null,null,0,null,true,null);

-- ----------------------------------------------------------------------- 
-- TABLE ROUNDTRIP_SMALL.ITEM 
-- ----------------------------------------------------------------------- 

CREATE TABLE ROUNDTRIP_SMALL.ITEM
(
    ITM_ID INTEGER,
    ITM_NAME VARCHAR(128),
    ITM_LONG_DESC VARCHAR(32672),
    ITM_FOREIGN_NAME VARCHAR(128),
    ITM_URL VARCHAR(1024),
    ITM_DISC_CD VARCHAR(64),
    ITM_UPC VARCHAR(64),
    ITM_WARRANTY VARCHAR(1),
    ITM_UNIT_PRICE DOUBLE,
    ITM_UNIT_COST DOUBLE,
    ITM_SUBCAT_ID INTEGER,
    ITM_SUPPLIER_ID INTEGER,
    ITM_BRAND_ID INTEGER,
    ITM_NAME_DE VARCHAR(128),
    ITM_NAME_FR VARCHAR(128),
    ITM_NAME_ES VARCHAR(128),
    ITM_NAME_IT VARCHAR(128),
    ITM_NAME_PO VARCHAR(128),
    ITM_NAME_JA VARCHAR(128),
    ITM_NAME_SCH VARCHAR(128),
    ITM_NAME_KO VARCHAR(128),
    ITM_LONG_DESC_DE VARCHAR(32672),
    ITM_LONG_DESC_FR VARCHAR(32672),
    ITM_LONG_DESC_ES VARCHAR(32672),
    ITM_LONG_DESC_IT VARCHAR(32672),
    ITM_LONG_DESC_PO VARCHAR(32672),
    ITM_ITM_LONG_DESC_JA VARCHAR(32672),
    ITM_LONG_DESC_SCH VARCHAR(32672),
    ITM_LONG_DESC_KO VARCHAR(32672)
);
call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_SMALL', 'ITEM', null, '<USER.DIR>/src/test/resources/small_msdatasample/item.csv', ',', '"', null,null,null,0,null,true,null);

-- ----------------------------------------------------------------------- 
-- TABLE ROUNDTRIP_SMALL.ORDER_HEADER 
-- ----------------------------------------------------------------------- 

CREATE TABLE ROUNDTRIP_SMALL.ORDER_HEADER
(
    ORH_ID VARCHAR(50),
    ORH_DATE TIMESTAMP,
    ORH_EMP_ID INTEGER,
    ORH_AMT DOUBLE,
    ORH_COST DOUBLE,
    ORH_QTY DOUBLE,
    ORH_SHIP_DATE TIMESTAMP,
    ORH_RUSH INTEGER
);
 call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_SMALL', 'ORDER_HEADER', null, '<USER.DIR>/src/test/resources/small_msdatasample/order_header.csv', ',', '"', null,null,null,0,null,true,null);

-- ----------------------------------------------------------------------- 
-- TABLE ROUNDTRIP_SMALL.ORDER_LINE 
-- ----------------------------------------------------------------------- 

CREATE TABLE ROUNDTRIP_SMALL.ORDER_LINE
(
    ORL_ORDER_ID VARCHAR(50),
    ORL_AMT INTEGER,
    ORL_ITEM_ID INTEGER,
    ORL_DATE TIMESTAMP,
    ORL_EMP_ID INTEGER,
    ORL_PROMOTION_ID INTEGER,
    ORL_QTY_SOLD INTEGER,
    ORL_UNIT_PRICE DOUBLE,
    ORL_UNIT_COST DOUBLE,
    ORL_DISCOUNT DOUBLE,
    ORL_CUSTOMER_ID INTEGER
);
 call SYSCS_UTIL.IMPORT_DATA ('ROUNDTRIP_SMALL', 'ORDER_LINE', null, '<USER.DIR>/src/test/resources/small_msdatasample/order_line.csv', ',', '"', null,null,null,0,null,true,null);

