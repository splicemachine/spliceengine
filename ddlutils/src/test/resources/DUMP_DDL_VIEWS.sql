-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_VIEWS
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_VIEWS AUTHORIZATION JOE;

SET SCHEMA DUMP_DDL_VIEWS;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.A
(
    SI VARCHAR(40),
    SA VARCHAR(40),
    SC VARCHAR(40),
    SD INTEGER,
    SE DOUBLE
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.CUSTOMER
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.CUSTOMER
(
    CST_ID INTEGER NOT NULL,
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
    CST_STATUS_ID INTEGER,
    PRIMARY KEY (CST_ID)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.D
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.D
(
    A VARCHAR(20),
    B VARCHAR(20),
    C VARCHAR(10),
    D DECIMAL(5,0),
    E VARCHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.E
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.E
(
    A VARCHAR(20),
    B VARCHAR(20),
    W DECIMAL(4,0),
    E VARCHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.HITS
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.HITS
(
    MY_DATE DATE,
    IP_ADDRESS CHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.ITEM
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.ITEM
(
    ITM_ID INTEGER NOT NULL,
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
    ITM_LONG_DESC_KO VARCHAR(32672),
    PRIMARY KEY (ITM_ID)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.ORDER_HEADER
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.ORDER_HEADER
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

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.UPUNIQ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.UPUNIQ
(
    NUMKEY INTEGER NOT NULL,
    COL2 VARCHAR(2)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS.WORKS
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS.WORKS
(
    EMPNUM VARCHAR(3) NOT NULL,
    PNUM VARCHAR(3) NOT NULL,
    HOURS DECIMAL(5,0)
);

-- -----------------------------------------------------------------------
-- VIEW DUMP_DDL_VIEWS.MONTHLY_HITS
-- -----------------------------------------------------------------------

CREATE VIEW DUMP_DDL_VIEWS.monthly_hits(month,ip_address,hits) as
 SELECT month(my_date), ip_address,
   COUNT(*)
 FROM DUMP_DDL_VIEWS.hits
 GROUP BY month(my_date),ip_address;

-- TODO: test cross-schema views
--CREATE VIEW DUMP_DDL_VIEWS.monthly_hits(month,ip_address,hits,SI) as
-- SELECT month(my_date), ip_address,
--   COUNT(*), DUMP_DDL_VIEWS_OTHER.A.SI
-- FROM DUMP_DDL_VIEWS.hits, DUMP_DDL_VIEWS_OTHER.A
-- GROUP BY month(my_date),ip_address,SI;

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_VIEWS_OTHER
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_VIEWS_OTHER AUTHORIZATION SUE;

SET SCHEMA DUMP_DDL_VIEWS_OTHER;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_VIEWS_OTHER.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_VIEWS_OTHER.A
(
    SI VARCHAR(40),
    SA VARCHAR(40),
    SC VARCHAR(40),
    SD INTEGER,
    SE DOUBLE
);

