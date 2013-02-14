package com.splicemachine.si.utils;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.HBaseConstants;

public class SIConstants {
	public static final byte[] ZERO_BYTE_ARRAY = new byte[0];
	public static final String SNAPSHOT_ISOLATION_FAMILY = "_si";
	public static final byte[] SNAPSHOT_ISOLATION_FAMILY_BYTES = SNAPSHOT_ISOLATION_FAMILY.getBytes();
	public static final String TRANSACTION_TABLE = "__TXN";
	public static final byte[] TRANSACTION_TABLE_BYTES = TRANSACTION_TABLE.getBytes();
	public static final byte[] START_TIMESTAMP_COLUMN = Bytes.toBytes(0);
	public static final byte[] COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(1);
	public static final byte[] TRANSACTION_STATUS_COLUMN = Bytes.toBytes(2);	
	public static final byte[] DEFAULT_FAMILY = HBaseConstants.DEFAULT_FAMILY.getBytes();
	public static final byte[] TRANSACTION_FAMILY = HBaseConstants.DEFAULT_FAMILY.getBytes();
	public static final byte[] SNAPSHOT_ISOLATION_RECORD_COLUMN = Bytes.toBytes(0);	
	public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(1);		
	public static final byte[] SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN = Bytes.toBytes(2);	
	public static final String WRITE_WRITE_CONFLICT_COMMIT = "Write-Write Conflict violates Snashot Isolation for put %s with existing commit timestamp %d";
	public static final String NO_TRANSACTION_STATUS = "Transaction with start timestamp %s does not have a transaction status";
}
