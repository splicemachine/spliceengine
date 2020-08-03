package com.splicemachine.ck;

public class Constants {
    public final static String SPLICE_PREFIX = "splice:";

    public final static String SPLICE_PATTERN = "splice.*";

    public final static String TBL_TABLES_COL0 = "hbase name";
    public final static String TBL_TABLES_COL1 = "schema";
    public final static String TBL_TABLES_COL2 = "table";
    public final static String TBL_TABLES_COL3 = "index";
    public final static String TBL_TABLES_COL4 = "create txn";

    public final static int TBL_TABLES_HBASE_NAME_IDX = 0;
    public final static int TBL_TABLES_SCHEMA_IDX = 1;
    public final static int TBL_TABLES_NAME_IDX = 2;
    public final static int TBL_TABLES_INDEX_IDX = 3;
    public final static int TBL_TABLES_CREATE_TXN_IDX = 4;

    public final static String TBL_TXN_COL0 = "transaction id";
    public final static String TBL_TXN_COL1 = "commit timestamp";
    public final static String TBL_TXN_COL2 = "global commit timestamp";
    public final static String TBL_TXN_COL3 = "parent transaction id";
    public final static String TBL_TXN_COL4 = "state";
    public final static String TBL_TXN_COL5 = "isolation level";
    public final static String TBL_TXN_COL6 = "is additive";
    public final static String TBL_TXN_COL7 = "keep alive time";
    public final static String TBL_TXN_COL8 = "rollback sub ids";
    public final static String TBL_TXN_COL9 = "target tables";

    public final static int TBL_TXN_ID_IDX = 0;
    public final static int TBL_TXN_COMMIT_TS_IDX = 1;
    public final static int TBL_TXN_GCOMMIT_TS_IDX = 2;
    public final static int TBL_TXN_PARENT_TX_ID_IDX = 3;
    public final static int TBL_TXN_STATE_IDX = 4;
    public final static int TBL_TXN_ISOLATION_LEVEL_IDX = 5;
    public final static int TBL_TXN_IS_ADDITIVE_IDX = 6;
    public final static int TBL_TXN_KEEP_ALIVE_IDX = 7;
    public final static int TBL_TXN_ROLLBACK_SUB_IDX = 8;
    public final static int TBL_TXN_TARGET_TABLES_IDX = 9;

    public final static String TBL_COLTABLE_COL0 = "column index";
    public final static String TBL_COLTABLE_COL1 = "column name";
    public final static String TBL_COLTABLE_COL2 = "column type";

    public final static String NULL = "NULL";
}
