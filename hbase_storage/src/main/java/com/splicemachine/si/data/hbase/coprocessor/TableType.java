package com.splicemachine.si.data.hbase.coprocessor;

/**
 * @author Scott Fines
 *         Date: 1/22/16
 */
public enum TableType{
    TRANSACTION_TABLE,
    ROOT_TABLE,
    META_TABLE,
    DERBY_SYS_TABLE,
    USER_TABLE,
    DDL_TABLE,
    HBASE_TABLE
}

