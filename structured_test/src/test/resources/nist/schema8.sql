-- SQL Test Suite, V6.0, Schema Definition, schema8.smi
-- 59-byte ID
-- TEd Version #
-- date_time print
-- ***************************************************************
-- ****** THIS FILE SHOULD BE RUN UNDER AUTHORIZATION ID SUN *****
-- ***************************************************************

-- This file defines the base tables used in most of the CDR tests.

-- This non-standard schema definition is provided so that
-- implementations which require semicolons to terminate statements,
-- but which are otherwise conforming, can still execute the
-- remaining tests.


  CREATE SCHEMA
--O      AUTHORIZATION SUN;
      SUN;
  set schema SUN;


  CREATE TABLE SUN.ECCO (C1 VARCHAR(2));
--O  CREATE TABLE ECCO (C1 VARCHAR(2));

  CREATE TABLE STAFF
-- Bug 202   
   (EMPNUM   VARCHAR(3) NOT NULL,
    EMPNAME  VARCHAR(20),
    GRADE    DECIMAL(4),
    CITY     VARCHAR(15));


  CREATE TABLE PROJ
-- Bug 202 
  (PNUM     VARCHAR(3) NOT NULL,
-- Bug 379    PNAME    CHAR(20),
    PNAME    VARCHAR(20),
-- Bug 379    PTYPE    CHAR(6),
    PTYPE    VARCHAR(6),
    BUDGET   DECIMAL(9),
    CITY     VARCHAR(15));


  CREATE TABLE WORKS (
-- Bug 202
    EMPNUM   VARCHAR(3) NOT NULL,
-- Bug 202 
    PNUM     VARCHAR(3) NOT NULL,
    HOURS    DECIMAL(5));

  CREATE TABLE STAFF3 (
-- Bug 202 
    EMPNUM   VARCHAR(3) NOT NULL,
    EMPNAME  VARCHAR(20),
    GRADE    DECIMAL(4),
    CITY     VARCHAR(15),
    primary key (EMPNUM));
--    UNIQUE (EMPNUM));


  CREATE TABLE PROJ3(
-- Bug 202   
   PNUM     VARCHAR(3) NOT NULL,
-- Bug 379    PNAME    CHAR(20),
    PNAME    VARCHAR(20),
-- Bug 379    PTYPE    VARCHAR(6),
    PTYPE    VARCHAR(6),
    BUDGET   DECIMAL(9),
    CITY     VARCHAR(15)
--    CONSTRAINT PROJ3_UNIQUE UNIQUE (PNUM)
);


  CREATE TABLE WORKS3
-- Bug 202
   (EMPNUM   VARCHAR(3) NOT NULL,
-- Bug 202
    PNUM     VARCHAR(3) NOT NULL,
    HOURS    DECIMAL(5));
--    FOREIGN KEY (EMPNUM) REFERENCES STAFF3(EMPNUM),
--    FOREIGN KEY (PNUM) REFERENCES PROJ3(PNUM));

-- Bug 202        
CREATE TABLE STAFF4 (EMPNUM    VARCHAR(3) NOT NULL,
-- Bug 379     EMPNAME  CHAR(20) DEFAULT NULL,
-- Bug 202                
                EMPNAME  VARCHAR(20) DEFAULT NULL,
                GRADE DECIMAL(4) DEFAULT 0,
                CITY   VARCHAR(15) DEFAULT '               '
--              GRADE DECIMAL(4) ,
--              CITY   VARCHAR(15)
 );


-- Bug 202     
       CREATE TABLE STAFF14 (EMPNUM    VARCHAR(3) NOT NULL,
--O                EMPNAME  VARCHAR(20) DEFAULT USER,
-- Bug 379                EMPNAME  VARCHAR(20) ,
                EMPNAME  VARCHAR(20) ,
        -- EMPNAME VARCHAR precision may be changed to implementation-defined
        --              precision for value of USER
                GRADE DECIMAL(4),
                CITY   VARCHAR(15));


-- Bug 202  
      CREATE TABLE STAFF5 (EMPNUM    VARCHAR(3) NOT NULL,
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15),
                PRIMARY KEY (EMPNUM));
-- Bug 263                CONSTRAINT STAFF5_GRADE CHECK (GRADE > 0 AND GRADE < 20));


-- Bug 202      
        CREATE TABLE STAFF6 (EMPNUM    VARCHAR(3) NOT NULL,
--Bug 379       EMPNAME  VARCHAR(20),
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
-- Bug 263      CONSTRAINT STAFF6_GRADE CHECK (GRADE > 0 AND GRADE < 20),
                CITY   VARCHAR(15));

-- Bug 202      
         CREATE TABLE STAFF7 (EMPNUM    VARCHAR(3) NOT NULL,
-- Bug 379    EMPNAME  VARCHAR(20),
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15),
                CONSTRAINT STAFF7_PK  PRIMARY KEY (EMPNUM));
-- Bug 263                CONSTRAINT STAFF7_GRADE CHECK (GRADE BETWEEN 1 AND 20));

-- Bug 202
              CREATE TABLE STAFF8 (EMPNUM    VARCHAR(3) NOT NULL,
-- Bug 379                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15),
                PRIMARY KEY (EMPNUM));
-- Bug 263                CONSTRAINT STAFF8_EMPNAME CHECK (EMPNAME IS NOT NULL));


-- Bug 202  
           CREATE TABLE STAFF9 (EMPNUM    VARCHAR(3) NOT NULL,
-- Bug 263                CONSTRAINT STAFF9_PK PRIMARY KEY,
-- Bug 379                EMPNAME  VARCHAR(20),
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15));
-- Bug 263                 CONSTRAINT STAFF9_EMPNAME CHECK (EMPNAME NOT LIKE 'T%'));


-- Bug 202  
          CREATE TABLE STAFF10 (EMPNUM    VARCHAR(3) NOT NULL,
-- Bug 379                EMPNAME  VARCHAR(20),
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15),
                PRIMARY KEY (EMPNUM));
-- Bug 263                 CONSTRAINT STAFF10_GRADE CHECK (GRADE NOT IN (5,22)));


        CREATE TABLE STAFF11 (EMPNUM    VARCHAR(3) NOT NULL PRIMARY KEY,
-- Bug 379                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15));
-- Bug 263                 CONSTRAINT STAFF11_GRADE_EMPNAME
-- Bug 263                 CHECK (GRADE NOT IN (5,22) 
-- Bug 263                             AND EMPNAME NOT LIKE 'T%'));


        CREATE TABLE STAFF12 (EMPNUM    VARCHAR(3) NOT NULL,
-- Bug 379                EMPNAME  VARCHAR(20),
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15),
                PRIMARY KEY (EMPNUM));
-- Bug 263                 CONSTRAINT STAFF12_GRADE_EMPNAME
-- Bug 263                 CHECK (NOT GRADE IN (5,22) 
-- Bug 263                             AND NOT EMPNAME LIKE 'T%'));


        CREATE TABLE STAFF13 (EMPNUM   VARCHAR(3) NOT NULL,
-- Bug 379                EMPNAME  VARCHAR(20),
                EMPNAME  VARCHAR(20),
                GRADE DECIMAL(4),
                CITY   VARCHAR(15),
                PRIMARY KEY (EMPNUM));
-- Bug 263                 CONSTRAINT STAFF13_EMPNAME CHECK (NOT EMPNAME IS NULL));

        CREATE TABLE STAFF15 (EMPNUM   VARCHAR(3),
-- Bug 379                EMPNAME  VARCHAR(20) NOT NULL,
                EMPNAME  VARCHAR(20) NOT NULL,
                GRADE DECIMAL(4),
                CITY   VARCHAR(15));
   
        CREATE TABLE STAFF16 (EMPNUM   VARCHAR(3) NOT NULL,
-- Bug 379,202
                   EMPNAME  VARCHAR(20) DEFAULT NULL,
-- Bug263 
                GRADE DECIMAL(4) NOT NULL CHECK (GRADE IN (100,150,200)),
--               GRADE DECIMAL(4) NOT NULL,
                CITY   VARCHAR(15), PRIMARY KEY (GRADE,EMPNUM));
                
        CREATE TABLE SIZ1_P
           (S1  VARCHAR(3),
            S2  VARCHAR(3),
            S3   DECIMAL(4),
            S4  VARCHAR(3),
            S5   DECIMAL(4),
            S6  VARCHAR(3),
            R1  VARCHAR(3),
            R2  VARCHAR(3),
            R3   DECIMAL(4));
--            UNIQUE   (S1,S2,S3,S4,S5,S6));
        
        
        CREATE TABLE SIZ1_F
           (F1  VARCHAR(3) NOT NULL,
            F2  VARCHAR(3),
            F3   DECIMAL(4),
            F4  VARCHAR(3),
            F5   DECIMAL(4),
            F6  VARCHAR(3),
            R1  VARCHAR(3),
            R2   DECIMAL(5),
            R3   DECIMAL(4));
--           FOREIGN KEY   (F1,F2,F3,F4,F5,F6)
--           REFERENCES SIZ1_P(S1,S2,S3,S4,S5,S6));
        
        
        
        CREATE TABLE SIZ2_P
           (P1  VARCHAR(3) NOT NULL,
            P2  VARCHAR(3) NOT NULL,
            P3   DECIMAL(4) NOT NULL,
            P4  VARCHAR(3) NOT NULL,
            P5   DECIMAL(4) NOT NULL,
            P6  VARCHAR(3) NOT NULL,
            P7  VARCHAR(3) NOT NULL,
            P8   DECIMAL(4) NOT NULL,
            P9   DECIMAL(4) NOT NULL,
            P10   DECIMAL(4) NOT NULL,
            P11   VARCHAR(4));
--            UNIQUE (P1),
--            UNIQUE (P2),
--            UNIQUE (P3),
--            UNIQUE (P4),
--            UNIQUE (P5),
--            UNIQUE (P6),
--            UNIQUE (P7),
--            UNIQUE (P8),
--            UNIQUE (P9),
--            UNIQUE (P10));
        
        
        CREATE TABLE SIZ2_F1
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P1));
        
        CREATE TABLE SIZ2_F2
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P2));
        
        CREATE TABLE SIZ2_F3
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P3));
        
        CREATE TABLE SIZ2_F4
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P4));
        
        CREATE TABLE SIZ2_F5
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P5));
        
        CREATE TABLE SIZ2_F6
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P6));
        
        CREATE TABLE SIZ2_F7
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P7));

        CREATE TABLE SIZ2_F8
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P8));
        
        CREATE TABLE SIZ2_F9
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P9));
        
        CREATE TABLE SIZ2_F10
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8));
--            FOREIGN KEY (F1) 
--            REFERENCES SIZ2_P(P10));
        
        
        CREATE TABLE SIZ3_P1
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P2
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P3
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8),
           primary key (F1));
--           UNIQUE (F1));
        
        CREATE TABLE SIZ3_P4
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P5
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P6
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P7
           (F1  VARCHAR(3) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P8
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P9
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_P10
           (F1   DECIMAL(4) NOT NULL,
            F2   VARCHAR(8),
            primary key (F1));
--            UNIQUE (F1));
        
        CREATE TABLE SIZ3_F
           (P1  VARCHAR(3) NOT NULL,
            P2  VARCHAR(3),
            P3   DECIMAL(4),
            P4  VARCHAR(3),
            P5   DECIMAL(4),
            P6  VARCHAR(3),
            P7  VARCHAR(3),
            P8   DECIMAL(4),
            P9   DECIMAL(4),
            P10   DECIMAL(4),
            P11   VARCHAR(4));
--            FOREIGN KEY (P1)
--            REFERENCES SIZ3_P1(F1),
--            FOREIGN KEY (P2)
--            REFERENCES SIZ3_P2(F1),
--            FOREIGN KEY (P3)
--            REFERENCES SIZ3_P3(F1),
--            FOREIGN KEY (P4)
--            REFERENCES SIZ3_P4(F1),
--            FOREIGN KEY (P5)
--            REFERENCES SIZ3_P5(F1),
--            FOREIGN KEY (P6)
--            REFERENCES SIZ3_P6(F1),
--            FOREIGN KEY (P7)
--            REFERENCES SIZ3_P7(F1),
--            FOREIGN KEY (P8)
--            REFERENCES SIZ3_P8(F1),
--            FOREIGN KEY (P9)
--            REFERENCES SIZ3_P9(F1),
--            FOREIGN KEY (P10)
--            REFERENCES SIZ3_P10(F1));

        CREATE TABLE DEPT
                (DNO DECIMAL(4) NOT NULL,
                 DNAME VARCHAR(20) NOT NULL,
                 DEAN VARCHAR(30),
                 PRIMARY KEY (DNO));
--                 UNIQUE (DNAME));
     
        CREATE TABLE EMP
                (ENO DECIMAL(4) NOT NULL,
                 ENAME VARCHAR(20) NOT NULL,
                 EDESC VARCHAR(30),
                 DNO DECIMAL(4) NOT NULL,
                 DNAME VARCHAR(20),
                 BTH_DATE  DECIMAL(6) NOT NULL,
                 PRIMARY KEY (ENO));
--                 UNIQUE (ENAME,BTH_DATE),
--                 FOREIGN KEY (DNO) REFERENCES
--                 DEPT(DNO),
--                 FOREIGN KEY (DNAME) REFERENCES
--                 DEPT(DNAME));
     
     
        CREATE TABLE EXPERIENCE
                (EXP_NAME VARCHAR(20),
                 BTH_DATE DECIMAL(6),
                 WK_DATE  DECIMAL(6),
                 DESCR VARCHAR(40));
--                 FOREIGN KEY (EXP_NAME,BTH_DATE) REFERENCES
--                 EMP(ENAME,BTH_DATE));
     
     -- The following tables, STAFF_M and PROJ_M reference each other.
     -- Table STAFF_M has a "forward reference" to PROJ_M.
                              
     CREATE TABLE STAFF_M
        (EMPNUM  VARCHAR(3) NOT NULL,
-- Bug 379          EMPNAME  VARCHAR(20),
         EMPNAME  VARCHAR(20),
         GRADE    DECIMAL(4),
         CITY     VARCHAR(15),
         PRI_WK  VARCHAR(3),
       primary key   (EMPNUM));
--         UNIQUE   (EMPNUM));


     
        CREATE TABLE PROJ_M
           (PNUM    VARCHAR(3) NOT NULL,
-- Bug 379    PNAME    VARCHAR(20),
           PNAME    VARCHAR(20),
-- Bug 379    PTYPE    VARCHAR(6),
           PTYPE    VARCHAR(6),
            BUDGET   DECIMAL(9),
            CITY     VARCHAR(15),
--            MGR  VARCHAR(3),
            MGR  VARCHAR(3) UNIQUE,
);

--            UNIQUE (PNUM),
--              CONSTRAINT fk_mgr
--            FOREIGN KEY (MGR)
--            REFERENCES STAFF_M(EMPNUM));

-- Removed alter table due to bug 537, made changes to the table above to handle-- foreign key constraint
--        ALTER TABLE STAFF_M ADD FOREIGN KEY (PRI_WK)
--          REFERENCES PROJ_M (PNUM);
     
	--   The following table is self-referencing.

        CREATE TABLE STAFF_C
           (EMPNUM  VARCHAR(3) NOT NULL,
-- Bug 379            EMPNAME  VARCHAR(20),
            EMPNAME  VARCHAR(20),
            GRADE    DECIMAL(4),
            CITY     VARCHAR(15),
            MGR  VARCHAR(3));

--            UNIQUE   (EMPNUM),
--            FOREIGN KEY (MGR)
--            REFERENCES STAFF_C(EMPNUM));
     
     

     CREATE TABLE STAFF_P
        (EMPNUM  VARCHAR(3) NOT NULL,
-- Bug 379         EMPNAME  VARCHAR(20),
         EMPNAME  VARCHAR(20),
         GRADE    DECIMAL(4),
         CITY     VARCHAR(15));
--       primary key  (EMPNUM));
--         UNIQUE  (EMPNUM));
     
     
     CREATE TABLE PROJ_P
        (PNUM    VARCHAR(3) NOT NULL,
-- Bug 379    PNAME    VARCHAR(20),
         PNAME    VARCHAR(20),
-- Bug 379    PTYPE    VARCHAR(6),
         PTYPE    VARCHAR(6),
         BUDGET   DECIMAL(9),
         CITY     VARCHAR(15));
--         primary key   (PNUM));
--         UNIQUE   (PNUM));
     
     
--        CREATE TABLE MID1 (P_KEY DECIMAL(4) NOT NULL UNIQUE,
        CREATE TABLE MID1 (P_KEY DECIMAL(4) NOT NULL, F_KEY DECIMAL(4));
--                         F_KEY DECIMAL(4) REFERENCES MID1(P_KEY));

--        CREATE TABLE ACR_SCH_P(P1 DECIMAL(4) NOT NULL UNIQUE,
        CREATE TABLE ACR_SCH_P(P1 DECIMAL(4) NOT NULL,
                               P2 VARCHAR(4));

--O   CREATE TABLE VARCHAR_DEFAULT
--O               (SEX_CODE  VARCHAR(1)  DEFAULT 'F',
--O                NICKNAME  VARCHAR(20) DEFAULT 'No nickname given',
--O                INSURANCE1 VARCHAR(5)  DEFAULT 'basic');

--O   CREATE TABLE EXACT_DEF
--O               (BODY_TEMP NUMERIC(4,1) DEFAULT 98.6,
--O                MAX_NUM   NUMERIC(5)   DEFAULT -55555,
--O                MIN_NUM   DEC(6,6)     DEFAULT .000001);

--O   CREATE TABLE APPROX_DEF
--O               (X_COUNT   REAL DEFAULT 1.78E12,
--O                Y_COUNT   REAL DEFAULT -9.99E10,
--O                Z_COUNT   REAL DEFAULT 3.45E-11,
--O                ZZ_COUNT  REAL DEFAULT -7.6777E-7);

--O   CREATE TABLE SIZE_TAB
--O               (COL1 VARCHAR(75)  DEFAULT 
--O'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz0123456789012',
--O                COL2 INTEGER   DEFAULT -999888777,
--O                COL3 DEC(15,6) DEFAULT 987654321.123456,
--O                COL4 REAL      DEFAULT -1.048576E22);   


   CREATE TABLE COMMODITY
         (C_NUM INTEGER NOT NULL,
--          C_NAME VARCHAR(7) NOT NULL UNIQUE,
          C_NAME VARCHAR(7) NOT NULL,
          PRIMARY KEY (C_NUM));

   CREATE TABLE CURRENCY_TABLE
         (CURRENCY VARCHAR(10) NOT NULL,
          DOLLAR_EQUIV NUMERIC(5, 2),
          PRIMARY KEY (CURRENCY));
                 
   CREATE TABLE MEASURE_TABLE
         (MEASURE VARCHAR(8) NOT NULL,
          POUND_EQUIV NUMERIC(8,2),
          PRIMARY KEY (MEASURE));

   CREATE TABLE C_TRANSACTION 
         (COMMOD_NO INTEGER,
          TOT_PRICE     DECIMAL(12,2),
          CURRENCY  VARCHAR(10),
          UNITS     INTEGER,
          MEASURE   VARCHAR(8),
          T_DATE    INTEGER);
--          FOREIGN KEY (COMMOD_NO)
--           REFERENCES COMMODITY,
--          FOREIGN KEY (CURRENCY)
--           REFERENCES CURRENCY_TABLE,
--          FOREIGN KEY (MEASURE)
--           REFERENCES MEASURE_TABLE);

  CREATE TABLE T6118REF (
       COL1 VARCHAR(20) NOT NULL, COL2 VARCHAR(20) NOT NULL,
       COL3 VARCHAR(20) NOT NULL, COL4 VARCHAR(20) NOT NULL,
       COL5 VARCHAR(23) NOT NULL, COL6 NUMERIC (4) NOT NULL,
       STR118 VARCHAR(118) NOT NULL);
--       STR118 VARCHAR(118) NOT NULL UNIQUE);
--       UNIQUE (COL1, COL2, COL4, COL3, COL5, COL6));

--  CREATE TABLE T118(STR118 VARCHAR(118) NOT NULL UNIQUE);
  CREATE TABLE T118(STR118 VARCHAR(118) NOT NULL);
  --  FOREIGN KEY (STR118) REFERENCES T6118REF (STR118));

  CREATE TABLE T6 (COL1 VARCHAR(20), COL2 VARCHAR(20),
                 COL3 VARCHAR(20), COL4 VARCHAR(20),
                 COL5 VARCHAR(23), COL6 NUMERIC (4));
--    FOREIGN KEY (COL1, COL2, COL4, COL3, COL5, COL6)
--      REFERENCES T6118REF (COL1, COL2, COL4, COL3, COL5, COL6));

-- ********************** create view statements *****************

  CREATE VIEW TESTREPORT AS
    SELECT TESTNO, RESULT, TESTTYPE
    FROM HU.TESTREPORT;
--O    FROM TESTREPORT;

--O   CREATE VIEW DOLLARS_PER_POUND (COMMODITY, UNIT_PRICE, FROM_DATE, TO_DATE)
--O      AS SELECT COMMODITY.C_NAME, 
--O                SUM(TOT_PRICE * DOLLAR_EQUIV) / SUM(UNITS * POUND_EQUIV),
--O                MIN(T_DATE), MAX(T_DATE)
--O         FROM C_TRANSACTION, COMMODITY, CURRENCY_TABLE, MEASURE_TABLE
--O         WHERE C_TRANSACTION.COMMOD_NO = COMMODITY.C_NUM
--O            AND C_TRANSACTION.CURRENCY = CURRENCY_TABLE.CURRENCY
--O            AND C_TRANSACTION.MEASURE  = MEASURE_TABLE.MEASURE
--O         GROUP BY COMMODITY.C_NAME
--O         HAVING SUM(TOT_PRICE * DOLLAR_EQUIV) > 10000;

-- View COST_PER_UNIT for OPTIONAL test 0403
-- Remove view from schema if it causes errors.

--O   CREATE VIEW COST_PER_UNIT
--O     (COMMODITY, UNIT_PRICE, CURRENCY, MEASURE)
--O      AS SELECT COMMODITY, UNIT_PRICE * POUND_EQUIV / DOLLAR_EQUIV,
--O                CURRENCY, MEASURE
--O            FROM DOLLARS_PER_POUND, CURRENCY_TABLE, MEASURE_TABLE;

        CREATE VIEW STAFF6_WITH_GRADES AS
                          SELECT EMPNUM,EMPNAME,GRADE,CITY
                          FROM STAFF6
                          WHERE GRADE > 0 AND GRADE < 20
  ;
--O                          WITH CHECK OPTION;

-- ************** grant statements follow *************
--O       GRANT SELECT ON SUN.ECCO TO PUBLIC;
        
--O        GRANT INSERT ON TESTREPORT
--O             TO PUBLIC;

--O        GRANT REFERENCES ON ACR_SCH_P TO SULLIVAN
--O              WITH GRANT OPTION;
        
--O        GRANT ALL PRIVILEGES ON PROJ_P
--O             TO SULLIVAN;

--O        GRANT ALL PRIVILEGES ON T6118REF TO FLATER;

--O        GRANT ALL PRIVILEGES ON T118 TO FLATER;

--O        GRANT ALL PRIVILEGES ON T6 TO FLATER;

--  Test GRANT without grant permission below.
--  "WITH GRANT OPTION" purposefully omitted from SUN's GRANT. 
--  Do not insert text "WITH GRANT OPTION"

--O        GRANT REFERENCES ON STAFF_P
--O             TO SULLIVAN;

--O        GRANT REFERENCES (C_NUM) ON COMMODITY TO SCHANZLE;

-- ************* End of Schema *************
