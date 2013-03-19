package com.splicemachine.si.utils;

import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.HBaseConstants;

public class SIConstants {
	public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
	public static final String SNAPSHOT_ISOLATION_FAMILY = "_si";
	public static final byte[] SNAPSHOT_ISOLATION_FAMILY_BYTES = SNAPSHOT_ISOLATION_FAMILY.getBytes();
	public static final String TRANSACTION_TABLE = "__TXN";
	public static final byte[] TRANSACTION_TABLE_BYTES = TRANSACTION_TABLE.getBytes();
    public static final int TRANSACTION_START_TIMESTAMP_COLUMN = 0;
    public static final int TRANSACTION_COMMIT_TIMESTAMP_COLUMN = 1;
    public static final int TRANSACTION_STATUS_COLUMN = 2;
	public static final byte[] TRANSACTION_START_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes(TRANSACTION_START_TIMESTAMP_COLUMN);
	public static final byte[] TRANSACTION_COMMIT_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes(TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
	public static final byte[] TRANSACTION_STATUS_COLUMN_BYTES = Bytes.toBytes(TRANSACTION_STATUS_COLUMN);
    public static final String DEFAULT_FAMILY = HBaseConstants.DEFAULT_FAMILY;
	public static final byte[] DEFAULT_FAMILY_BYTES = DEFAULT_FAMILY.getBytes();
	public static final byte[] TRANSACTION_FAMILY_BYTES = DEFAULT_FAMILY.getBytes();
    public static final int SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN = 0;
    public static final int SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN = 1;
	public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN);
	public static final byte[] SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES = Bytes.toBytes(SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN);
	public static final String WRITE_WRITE_CONFLICT_COMMIT = "Write-Write Conflict violates Snashot Isolation for mutation %s";
	public static final String NO_TRANSACTION_STATUS = "Transaction with start timestamp %s does not have a transaction status";
	public static final String FILTER_CHECKING_MULTIPLE_ROW_TOMBSTONES = "Filter should never hit multiple row level tombstones, one tombstone hit we move on.";
	public static final String SI = "SI";
}
