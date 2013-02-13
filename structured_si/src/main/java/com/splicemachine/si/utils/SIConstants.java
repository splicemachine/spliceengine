package com.splicemachine.si.utils;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.HBaseConstants;

public class SIConstants {
	public static final byte[] SNAPSHOT_ISOLATION = "_snapshotIsolation".getBytes();
	public static final String TRANSACTION_TABLE = "TXN";
	public static final byte[] TRANSACTION_TABLE_BYTES = TRANSACTION_TABLE.getBytes();
	public static final byte[] START_TIMESTAMP_COLUMN = Bytes.toBytes(0);
	public static final byte[] COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(1);
	public static final byte[] TRANSACTION_STATUS_COLUMN = Bytes.toBytes(2);	
	public static final byte[] TRANSACTION_FAMILY = HBaseConstants.DEFAULT_FAMILY.getBytes();
}
