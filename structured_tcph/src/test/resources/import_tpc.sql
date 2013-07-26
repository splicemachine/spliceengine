
-- edit import command to specify full file path

create schema tpch1x;
set scehema tpch1x;
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

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl','|','"', null); 
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.1','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.2','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.3','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.4','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.5','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.6','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.7','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem.tbl.8','|','"', null);

--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem_10.tbl','|','"', null);

--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem_10.tbl','|','"', null,0);

--call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'LINEITEM', null, null, '/Users/aramaswamy/splice/test/performance/tpch/lineitem_10.tbl','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

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

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;
                           
--splicetest: ignorediff start
SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'ORDERS', null, null, '/Users/aramaswamy/splice/test/performance/tpch/orders.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'ORDERS', null, null, '/Users/aramaswamy/splice/test/performance/tpch/orders.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'ORDERS', null, null, '/Users/aramaswamy/splice/test/performance/tpch/orders.tbl.1','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'ORDERS', null, null, '/Users/aramaswamy/splice/test/performance/tpch/orders.tbl.2','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'ORDERS', null, null, '/Users/aramaswamy/splice/test/performance/tpch/orders.tbl.8','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

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

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'CUSOTMER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/customer.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'CUSTOMER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/customer.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'CUSTOMER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/customer.tbl.1','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'CUSTOMER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/customer.tbl.2','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'CUSTOMER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/customer.tbl.8','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

-- 800000 rows populated
CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL ,
                             PS_SUPPKEY     INTEGER NOT NULL ,
                             PS_AVAILQTY    INTEGER,
                             PS_SUPPLYCOST  DECIMAL(15,2),
                             PS_COMMENT     VARCHAR(199),
                        PRIMARY KEY(PS_PARTKEY,PS_SUPPKEY)
                      );
--          ,FOREIGN KEY(PS_PARTKEY,PS_SUPPKEY) REFERENCES LINEITEM(L_PARTKEY,L_SUPPKEY));

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PARTSUPP', null, null, '/Users/aramaswamy/splice/test/performance/tpch/partsupp.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'PARTSUPP', null, null, '/Users/aramaswamy/splice/test/performance/tpch/partsupp.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PARTSUPP', null, null, '/Users/aramaswamy/splice/test/performance/tpch/partsupp.tbl.1','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PARTSUPP', null, null, '/Users/aramaswamy/splice/test/performance/tpch/partsupp.tbl.2','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PARTSUPP', null, null, '/Users/aramaswamy/splice/test/performance/tpch/partsupp.tbl.8','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;
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

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;
--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'SUPPLIER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/supplier.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'SUPPLIER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/supplier.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'SUPPLIER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/supplier.tbl.1','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'SUPPLIER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/supplier.tbl.2','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'SUPPLIER', null, null, '/Users/aramaswamy/splice/test/performance/tpch/supplier.tbl.8','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

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

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PART', null, null, '/Users/aramaswamy/splice/test/performance/tpch/part.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'PART', null, null, '/Users/aramaswamy/splice/test/performance/tpch/part.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PART', null, null, '/Users/aramaswamy/splice/test/performance/tpch/part.tbl.1','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PART', null, null, '/Users/aramaswamy/splice/test/performance/tpch/part.tbl.2','|','"', null);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'PART', null, null, '/Users/aramaswamy/splice/test/performance/tpch/part.tbl.8','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

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


SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'NATION', null, null, '/Users/aramaswamy/splice/test/performance/tpch/nation.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'NATION', null, null, '/Users/aramaswamy/splice/test/performance/tpch/nation.tbl','|','"', null,0);
call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'NATION', null, null, '/Users/aramaswamy/splice/test/performance/tpch/nation.tbl','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

-- 5 regions are populated.(AFRICA,ASIA,AMERICA,EUROPE,MIDDLE EAST
CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL PRIMARY KEY,
--                            R_NAME       CHAR(25),
                            R_NAME       VARCHAR(25),
                            R_COMMENT    VARCHAR(152));
--            FOREIGN KEY(R_REGIONKEY) references NATION(N_REGIONKEY);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

--splicetest: splice sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'REGION', null, null, '/Users/aramaswamy/splice/test/performance/tpch/region.tbl','|','"', null);
--splicetest: derby sql: call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, 'REGION', null, null, '/Users/aramaswamy/splice/test/performance/tpch/region.tbl','|','"', null,0);

call SYSCS_UTIL.SYSCS_IMPORT_DATA ('tpch1x', 'REGION', null, null, '/Users/aramaswamy/splice/test/performance/tpch/region.tbl','|','"', null);

SELECT CURRENT_TIMESTAMP FROM SYSIBM.SYSDUMMY1;

SELECT CONGLOMERATENAME FROM SYS.SYSCONGLOMERATES,
SYS.SYSCONSTRAINTS WHERE
SYS.SYSCONGLOMERATES.TABLEID = SYS.SYSCONSTRAINTS.TABLEID
AND CONSTRAINTNAME = 'LI_UNIQ';
