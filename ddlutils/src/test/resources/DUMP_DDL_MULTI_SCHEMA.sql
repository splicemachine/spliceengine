-- ----------------------------------------------------------------------- 
--  SCHEMA DUMP_DDL_MULTI_E
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_E AUTHORIZATION WILMA;

SET SCHEMA DUMP_DDL_MULTI_E;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.CHILDT
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.CHILDT
(
    I INTEGER,
    J INTEGER,
    K INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.COLS
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.COLS
(
    ID VARCHAR(128) NOT NULL,
    COLLID SMALLINT NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.CONTENT
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.CONTENT
(
    ID INTEGER NOT NULL,
    CREATOR VARCHAR(128) NOT NULL,
    CREATION_DATE DATE NOT NULL,
    URL VARCHAR(256) NOT NULL,
    TITLE VARCHAR(128) NOT NULL,
    DESCRIPTION VARCHAR(512) NOT NULL,
    HEIGHT INTEGER NOT NULL,
    WIDTH INTEGER NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.CONTENT_STYLE
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.CONTENT_STYLE
(
    CONTENT_ID INTEGER NOT NULL,
    STYLE_ID INTEGER NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.DOCS
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.DOCS
(
    ID VARCHAR(128) NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.KEYGEN
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.KEYGEN
(
    KEYVAL INTEGER NOT NULL,
    KEYNAME VARCHAR(256) NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.PARENTT
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.PARENTT
(
    I INTEGER,
    J INTEGER,
    K INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.PROJ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.PROJ
(
    PNUM VARCHAR(3) NOT NULL,
    PNAME VARCHAR(20),
    PTYPE VARCHAR(6),
    BUDGET DECIMAL(9,0),
    CITY VARCHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.RATING
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.RATING
(
    ID INTEGER NOT NULL,
    RATING DOUBLE NOT NULL,
    ENTRIES DOUBLE NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.S
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.S
(
    A INTEGER,
    B INTEGER,
    C INTEGER,
    D INTEGER,
    E INTEGER,
    F INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.STAFF
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.STAFF
(
    EMPNUM VARCHAR(3) NOT NULL,
    EMPNAME VARCHAR(20),
    GRADE DECIMAL(4,0),
    CITY VARCHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.STYLE
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.STYLE
(
    ID INTEGER NOT NULL,
    DESCRIPTION VARCHAR(128) NOT NULL
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.T1
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.T1
(
    K INTEGER,
    L INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.T2
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.T2
(
    K INTEGER,
    L INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.T3
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.T3
(
    I INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.T4
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.T4
(
    I INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.UPUNIQ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.UPUNIQ
(
    NUMKEY INTEGER NOT NULL,
    COL2 VARCHAR(2)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.WORKS
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.WORKS
(
    EMPNUM VARCHAR(3) NOT NULL,
    PNUM VARCHAR(3) NOT NULL,
    HOURS DECIMAL(5,0)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.Z1
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.Z1
(
    I INTEGER,
    S SMALLINT,
    C CHAR(30),
    VC CHAR(30),
    B BIGINT
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_E.Z2
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_E.Z2
(
    I INTEGER,
    S SMALLINT,
    C CHAR(30),
    VC CHAR(30),
    B BIGINT
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_D
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_D AUTHORIZATION BETTY;

SET SCHEMA DUMP_DDL_MULTI_D;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_D.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_D.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_D.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_D.C
(
    NAME VARCHAR(20),
    SURNAME VARCHAR(20)
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_G
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_G AUTHORIZATION BARNEY;

SET SCHEMA DUMP_DDL_MULTI_G;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_G.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_G.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_G.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_G.B
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_G.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_G.C
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_G.D
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_G.D
(
    D1 INTEGER,
    D2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_G.EMPTY_TABLE
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_G.EMPTY_TABLE
(
    E1 INTEGER,
    E2 INTEGER,
    E3 INTEGER,
    E4 INTEGER
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_F
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_F AUTHORIZATION FRED;

SET SCHEMA DUMP_DDL_MULTI_F;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.AA
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.AA
(
    A1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.B
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.BB
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.BB
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.C
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.CC
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.CC
(
    C1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.D
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.D
(
    D1 INTEGER,
    D2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.EMPTY_TABLE
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.EMPTY_TABLE
(
    E1 INTEGER,
    E2 INTEGER,
    E3 INTEGER,
    E4 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.Y
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.Y
(
    Y1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.YY
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.YY
(
    Y1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.Z
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.Z
(
    Z1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_F.ZZ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_F.ZZ
(
    Z1 INTEGER
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_I
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_I AUTHORIZATION JERRY;

SET SCHEMA DUMP_DDL_MULTI_I;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_I.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_I.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_I.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_I.B
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_I.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_I.C
(
    NAME VARCHAR(20),
    SURNAME VARCHAR(20)
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_H
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_H AUTHORIZATION TOM;

SET SCHEMA DUMP_DDL_MULTI_H;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_H.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_H.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_H.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_H.B
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_H.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_H.C
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_H.D
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_H.D
(
    D1 INTEGER,
    D2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_H.EMPTY_TABLE
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_H.EMPTY_TABLE
(
    E1 INTEGER,
    E2 INTEGER,
    E3 INTEGER,
    E4 INTEGER
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_K
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_K AUTHORIZATION HERMAN;

SET SCHEMA DUMP_DDL_MULTI_K;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.B
(
    B1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.C
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.D
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.D
(
    D1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.PROJ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.PROJ
(
    PNUM VARCHAR(3) NOT NULL,
    PNAME VARCHAR(20),
    PTYPE VARCHAR(6),
    BUDGET DECIMAL(9,0),
    CITY VARCHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.ST1
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.ST1
(
    TOID INTEGER,
    RD INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.ST2
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.ST2
(
    USERID INTEGER,
    PMNEW INTEGER,
    PMTOTAL INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.STAFF
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.STAFF
(
    EMPNUM VARCHAR(3) NOT NULL,
    EMPNAME VARCHAR(20),
    GRADE DECIMAL(4,0),
    CITY VARCHAR(15)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.T1
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.T1
(
    K INTEGER,
    L INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.T2
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.T2
(
    K INTEGER,
    L INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.T3
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.T3
(
    I INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.T4
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.T4
(
    I INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.T5
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.T5
(
    K INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.TWITHNULLS1
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.TWITHNULLS1
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.TWITHNULLS2
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.TWITHNULLS2
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.UPUNIQ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.UPUNIQ
(
    NUMKEY INTEGER NOT NULL,
    COL2 VARCHAR(2)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.WORKS
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.WORKS
(
    EMPNUM VARCHAR(3) NOT NULL,
    PNUM VARCHAR(3) NOT NULL,
    HOURS DECIMAL(5,0)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_K.WORKS8
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_K.WORKS8
(
    EMPNUM VARCHAR(3) NOT NULL,
    PNUM VARCHAR(3) NOT NULL,
    HOURS DECIMAL(5,0)
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_J
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_J AUTHORIZATION SPLICE;

SET SCHEMA DUMP_DDL_MULTI_J;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_J.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_J.A
(
    A1 INTEGER,
    A2 INTEGER,
    A3 INTEGER
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_A
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_A AUTHORIZATION CHICKEN;

SET SCHEMA DUMP_DDL_MULTI_A;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.A
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.AA
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.AA
(
    A1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.B
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.BB
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.BB
(
    B1 INTEGER,
    B2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.C
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.CC
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.CC
(
    C1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.D
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.D
(
    D1 INTEGER,
    D2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.DIFFROW
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.DIFFROW
(
    K INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.EMPTY
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.EMPTY
(
    I INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.EMPTY_TABLE
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.EMPTY_TABLE
(
    E1 INTEGER,
    E2 INTEGER,
    E3 INTEGER,
    E4 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.ONEROW
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.ONEROW
(
    J INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.Y
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.Y
(
    Y1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.YY
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.YY
(
    Y1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.Z
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.Z
(
    Z1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_A.ZZ
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_A.ZZ
(
    Z1 INTEGER
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_C
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_C AUTHORIZATION PERL;

SET SCHEMA DUMP_DDL_MULTI_C;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_C.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_C.A
(
    ACCOUNT_ID VARCHAR(75),
    FIRST_NAME VARCHAR(25),
    LAST_NAME VARCHAR(25)
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_C.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_C.B
(
    ACCOUNT_ID BIGINT,
    TRANS_AMT DECIMAL(10,2)
);

-- -----------------------------------------------------------------------
--  SCHEMA DUMP_DDL_MULTI_B
-- -----------------------------------------------------------------------

CREATE SCHEMA DUMP_DDL_MULTI_B AUTHORIZATION GRANDMA;

SET SCHEMA DUMP_DDL_MULTI_B;

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.A
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.A
(
    A1 INTEGER,
    A2 INTEGER,
    A3 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.B
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.B
(
    B1 INTEGER,
    B2 INTEGER,
    B3 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.C
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.C
(
    C1 INTEGER,
    C2 INTEGER,
    C3 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.P1
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.P1
(
    A1 INTEGER,
    A2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.P2
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.P2
(
    B1 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.P3
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.P3
(
    C1 INTEGER,
    C2 INTEGER
);

-- -----------------------------------------------------------------------
-- TABLE DUMP_DDL_MULTI_B.P4
-- -----------------------------------------------------------------------

CREATE TABLE DUMP_DDL_MULTI_B.P4
(
    D1 INTEGER
);

