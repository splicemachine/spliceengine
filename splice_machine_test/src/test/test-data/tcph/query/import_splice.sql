connect 'jdbc:derby://localhost:1527/splicedb';

-- edit import command to specify full file path

create schema tpch1x;
set schema tpch1x;
-- Sccsid:     @(#)dss.ddl	2.1.8.1
-- 6001215 rows populated
CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                             L_PARTKEY     INTEGER NOT NULL,
                             L_SUPPKEY     INTEGER NOT NULL,
                             L_LINENUMBER  INTEGER NOT NULL,
                             L_QUANTITY    DECIMAL(15,2),
                             L_EXTENDEDPRICE  DECIMAL(15,2),
                             L_DISCOUNT    DECIMAL(15,2),
                             L_TAX         DECIMAL(15,2),
                             L_RETURNFLAG  CHAR(1),
                             L_LINESTATUS  CHAR(1),
                             L_SHIPDATE    DATE,
                             L_COMMITDATE  DATE,
                             L_RECEIPTDATE DATE,
                             L_SHIPINSTRUCT CHAR(25),
                             L_SHIPMODE     CHAR(10),
                             L_COMMENT      VARCHAR(44),
                             PRIMARY KEY(L_ORDERKEY,L_LINENUMBER)
                      );
--             ,FOREIGN KEY(L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY));
-- 
VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'LINEITEM', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/lineitem.tbl','|','"', null,null,null);

--splicetest: ignorediff start
--splicetest: splice sql:call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/data/lineitem.tbl','|','"', null,null,null);

--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'LINEITEM', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/lineitem.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;

-- 1500000 rows populated.
CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL PRIMARY KEY,
                           O_CUSTKEY        INTEGER,
                           O_ORDERSTATUS    CHAR(1),
                           O_TOTALPRICE     DECIMAL(15,2),
                           O_ORDERDATE      DATE,
                           O_ORDERPRIORITY  CHAR(15),
                           O_CLERK          CHAR(15),
                           O_SHIPPRIORITY   INTEGER ,
                           O_COMMENT        VARCHAR(79)
                    );
--             ,FOREIGN KEY(O_CUSTKEY) REFERENCES LINEITEM(C_CUSTKEY));

VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'ORDERS', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/orders.tbl','|','"', null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'ORDERS', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/orders.tbl','|','"', null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'ORDERS', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/orders.tbl','|','"', null,0);
                           
VALUES (CURRENT_TIMESTAMP) ;

-- 150,000 rows populated
CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL PRIMARY KEY,
                             C_NAME        VARCHAR(25),
                             C_ADDRESS     VARCHAR(40),
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15),
                             C_ACCTBAL     DECIMAL(15,2),
                             C_MKTSEGMENT  CHAR(10),
                             C_COMMENT     VARCHAR(117)
                      );
--          ,FOREIGN KEY(C_CUSTKEY) REFERENCES ORDERS(O_CUSTKEY));
VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'CUSTOMER', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/customer.tbl','|','"', null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'CUSTOMER', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/customer.tbl','|','"', null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'CUSTOMER', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/customer.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;

-- 800000 rows populated
CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL ,
                             PS_SUPPKEY     INTEGER NOT NULL ,
                             PS_AVAILQTY    INTEGER,
                             PS_SUPPLYCOST  DECIMAL(15,2),
                             PS_COMMENT     VARCHAR(199),
                        PRIMARY KEY(PS_PARTKEY,PS_SUPPKEY)
                      );
--          ,FOREIGN KEY(PS_PARTKEY,PS_SUPPKEY) REFERENCES LINEITEM(L_PARTKEY,L_SUPPKEY));
VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'PARTSUPP', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/partsupp.tbl','|','"', null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'PARTSUPP', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/partsupp.tbl','|','"', null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'PARTSUPP', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/partsupp.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;
-- 10,000 populated
CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL PRIMARY KEY,
--                             S_NAME        CHAR(25) ,
                             S_NAME        VARCHAR(25) ,
                             S_ADDRESS     VARCHAR(40) ,
                             S_NATIONKEY   INTEGER ,
                             S_PHONE       CHAR(15) ,
                             S_ACCTBAL     DECIMAL(15,2),
                             S_COMMENT     VARCHAR(101)
                      );
--            ,FOREIGN KEY (S_SUPPKEY) REFERENCES  PARTSUPP(PS_SUPPKEY));

VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'SUPPLIER', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/supplier.tbl','|','"', null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'SUPPLIER', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/supplier.tbl','|','"', null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'SUPPLIER', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/supplier.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;

-- 200,000 populated
CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL PRIMARY KEY,
                          P_NAME        VARCHAR(55) ,
                          P_MFGR        CHAR(25) ,
                          P_BRAND       CHAR(10) ,
                          P_TYPE        VARCHAR(25) ,
                          P_SIZE        INTEGER ,
                          P_CONTAINER   CHAR(10) ,
                          P_RETAILPRICE DECIMAL(15,2),
                          P_COMMENT     VARCHAR(23)
                   );
--            ,FOREIGN KEY (P_PARTKEY) REFERENCES PARTSUPP(PS_PARTKEY));

VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'PART', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/part.tbl','|',NULL, null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'PART', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/part.tbl','|',NULL, null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'PART', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/part.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;

-- 25 rows populated.
CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
--                            N_NAME       CHAR(25),
                            N_NAME       VARCHAR(25),
                            N_REGIONKEY  INTEGER NOT NULL,
                            N_COMMENT    VARCHAR(152),
                            primary key (N_NATIONKEY)
                     );
--           ,FOREIGN KEY(N_NATIONKEY) REFERENCES CUSTOMER(C_NATIONKEY),
--            FOREIGN KEY(N_NATIONKEY) REFERENCES SUPPLIER(S_NATIONKEY));

VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'NATION', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/nation.tbl','|','"', null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'NATION', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/nation.tbl','|','"', null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'NATION', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/nation.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;

-- 5 regions are populated.(AFRICA,ASIA,AMERICA,EUROPE,MIDDLE EAST
CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY,
--                            R_NAME       CHAR(25),
                            R_NAME       VARCHAR(25),
                            R_COMMENT    VARCHAR(152));
--            FOREIGN KEY(R_REGIONKEY) references NATION(N_REGIONKEY);

VALUES (CURRENT_TIMESTAMP) ;

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'REGION', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/region.tbl','|','"', null,null,null);
--splicetest: ignorediff start
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'REGION', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/region.tbl','|','"', null,null,null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('TPCH1X', 'REGION', null, null, '/Users/nnikoo/git/test/new/test/performance/tpch/data/region.tbl','|','"', null,0);

VALUES (CURRENT_TIMESTAMP) ;

select count(*) from lineitem;
select count(*) from orders;
select count(*) from supplier;
select count(*) from part;
select count(*) from customer;
select count(*) from part;
select count(*) from region;
select count(*) from nation;
