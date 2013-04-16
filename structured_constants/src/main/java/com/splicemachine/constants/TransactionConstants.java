package com.splicemachine.constants;

import org.apache.hadoop.hbase.util.Bytes;

public class TransactionConstants extends HBaseConstants {
    public static final String TRANSACTION_PATH_NAME = "hbase.transaction.path";
    public static final String DEFAULT_TRANSACTION_PATH = "/transactions";

    public static final String TRANSACTION_TABLE = "__TXN";
    public static final byte[] TRANSACTION_TABLE_BYTES = Bytes.toBytes(TRANSACTION_TABLE);

    public static enum TableEnv {
    	TRANSACTION_TABLE,
    	ROOT_TABLE,
    	META_TABLE,
    	DERBY_SYS_TABLE,
    	USER_INDEX_TABLE,
    	USER_TABLE
    }
}
