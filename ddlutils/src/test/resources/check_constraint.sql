-- -----------------------------------------------------------------------
--  SCHEMA CHECK_CONSTRAINT
-- -----------------------------------------------------------------------

CREATE SCHEMA CHECK_CONSTRAINT AUTHORIZATION SPLICE;

SET SCHEMA CHECK_CONSTRAINT;

---- -----------------------------------------------------------------------
---- TABLE CHECK_CONSTRAINT.ALL_COLS_CHECKED
---- -----------------------------------------------------------------------

CREATE TABLE CHECK_CONSTRAINT.ALL_COLS_CHECKED
(
    JOIN_DATE DATE NOT NULL,
    ID INTEGER NOT NULL CONSTRAINT ID_GE_CK1 CHECK (ID >= 1000),
    I_GT INTEGER CONSTRAINT I_GT_CK1 CHECK (I_GT > 100),
    I_EQ INTEGER CONSTRAINT I_EQ_CK1 CHECK (I_EQ = 90),
    I_LT INTEGER CONSTRAINT I_LT_CK1 CHECK (I_LT < 80),
    V_NEQ VARCHAR(32) CONSTRAINT V_NEQ_CK1 CHECK (V_NEQ <> 'bad'),
    V_IN VARCHAR(32) CONSTRAINT V_IN_CK1 CHECK (V_IN IN ('good1', 'good2', 'good3')),
    PRIMARY KEY (JOIN_DATE, ID)
);

-- -----------------------------------------------------------------------
-- TABLE CHECK_CONSTRAINT.COLUMN_CONSTRAINT
-- -----------------------------------------------------------------------

CREATE TABLE CHECK_CONSTRAINT.TABLE_CONSTRAINT
(
    EFFECTIVE_START_DATE DATE NOT NULL,
    EFFECTIVE_END_DATE DATE NOT NULL,
    CONSTRAINT GOOD_DATE CHECK (EFFECTIVE_END_DATE > EFFECTIVE_START_DATE)

);

-- -----------------------------------------------------------------------
-- TABLE CHECK_CONSTRAINT.CHECKED2
-- -----------------------------------------------------------------------

CREATE TABLE CHECK_CONSTRAINT.COLUMN_CONSTRAINT
(
    KEY_COL INT NOT NULL PRIMARY KEY CONSTRAINT CONSTR_PK CHECK (KEY_COL>=1),
    INT2_COL INT NOT NULL CONSTRAINT CONSTR_INT2 CHECK (INT2_COL<5)
);

