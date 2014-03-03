package com.splicemachine.constants;

import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Defines the schema used by SI for the transaction table and for additional metadata on data tables.
 */

public class SIConstants extends SpliceConstants {
    static {
        setParameters();
    }

	public static final byte[] TRUE_BYTES = Bytes.toBytes(true);
	public static final byte[] FALSE_BYTES = Bytes.toBytes(false);
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] SNAPSHOT_ISOLATION_FAILED_TIMESTAMP = new byte[] {-1};
    public static final int TRANSACTION_ID_COLUMN = 14;
    public static final int TRANSACTION_START_TIMESTAMP_COLUMN = 0;
    public static final int TRANSACTION_PARENT_COLUMN = 1;
    public static final int TRANSACTION_DEPENDENT_COLUMN = 2;
    public static final int TRANSACTION_ALLOW_WRITES_COLUMN = 3;
    public static final int TRANSACTION_ADDITIVE_COLUMN = 17;
    public static final int TRANSACTION_READ_UNCOMMITTED_COLUMN = 4;
    public static final int TRANSACTION_READ_COMMITTED_COLUMN = 5;
    public static final int TRANSACTION_STATUS_COLUMN = 6;
    public static final int TRANSACTION_COMMIT_TIMESTAMP_COLUMN = 7;
    public static final int TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN = 16;
    public static final int TRANSACTION_KEEP_ALIVE_COLUMN = 8;
    public static final int TRANSACTION_COUNTER_COLUMN = 15;
	public static final int WRITE_TABLE_COLUMN = 18;

    public static final byte[] TRANSACTION_ID_COLUMN_BYTES = Encoding.encode(TRANSACTION_ID_COLUMN);
    public static final byte[] TRANSACTION_START_TIMESTAMP_COLUMN_BYTES = Encoding.encode(TRANSACTION_START_TIMESTAMP_COLUMN);
    public static final byte[] TRANSACTION_PARENT_COLUMN_BYTES = Encoding.encode(TRANSACTION_PARENT_COLUMN);
    public static final byte[] TRANSACTION_DEPENDENT_COLUMN_BYTES = Encoding.encode(TRANSACTION_DEPENDENT_COLUMN);
    public static final byte[] TRANSACTION_ALLOW_WRITES_COLUMN_BYTES = Encoding.encode(TRANSACTION_ALLOW_WRITES_COLUMN);
    public static final byte[] TRANSACTION_ADDITIVE_COLUMN_BYTES = Encoding.encode(TRANSACTION_ADDITIVE_COLUMN);
    public static final byte[] TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES = Encoding.encode(TRANSACTION_READ_UNCOMMITTED_COLUMN);
    public static final byte[] TRANSACTION_READ_COMMITTED_COLUMN_BYTES = Encoding.encode(TRANSACTION_READ_COMMITTED_COLUMN);
    public static final byte[] TRANSACTION_STATUS_COLUMN_BYTES = Encoding.encode(TRANSACTION_STATUS_COLUMN);
    public static final byte[] TRANSACTION_COMMIT_TIMESTAMP_COLUMN_BYTES = Encoding.encode(TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    public static final byte[] TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN_BYTES = Encoding.encode(TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    public static final byte[] TRANSACTION_KEEP_ALIVE_COLUMN_BYTES = Encoding.encode(TRANSACTION_KEEP_ALIVE_COLUMN);
    public static final byte[] TRANSACTION_COUNTER_COLUMN_BYTES = Encoding.encode(TRANSACTION_COUNTER_COLUMN);

    public static final byte[] TRANSACTION_FAMILY_BYTES = SpliceConstants.DEFAULT_FAMILY.getBytes();
	public static final int SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE = 0;
    public static final int SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN = 0;
    public static final int SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN = 1;
    public static final String SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING = SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN + "";
    public static final String SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING = SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN + "";
    public static final String SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_STRING = SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE + "";
    
    public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES = Bytes.toBytes(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING);
	public static final byte[] SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES = Bytes.toBytes(SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING);
	public static final byte[] SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES = Bytes.toBytes(SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_STRING);		
    public static final short SI_NEEDED_VALUE = (short) 0;
	public static final byte[] SI_NEEDED_VALUE_BYTES = Bytes.toBytes(SI_NEEDED_VALUE);

		@Parameter public static final String TRANSACTION_KEEP_ALIVE_INTERVAL = "splice.txn.keepAliveIntervalMs";
		@DefaultValue(TRANSACTION_KEEP_ALIVE_INTERVAL) public static final int DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL=60000;
    public static int transactionKeepAliveInterval;

		@Parameter public static final String TRANSACTION_TIMEOUT = "splice.txn.timeout";
		@DefaultValue(TRANSACTION_TIMEOUT) public static final int DEFAULT_TRANSACTION_TIMEOUT = 100 * DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL; //100 minutes
    public static int transactionTimeout;

    public static final String SI_TRANSACTION_ID_KEY = "A";
    public static final String SI_NEEDED = "B";
    public static final String SI_DELETE_PUT = "D";
    public static final String SI_COUNT_STAR = "M";
    
    
		/*
		 * The time, in ms, to wait between attempts at resolving a transaction which
		 * is in the COMMITTING state (e.g. the amount of time to wait for a transaction
		 * to move from COMMITTING to COMMITTED).
		 */
		@SpliceConstants.Parameter public static final String COMMITTING_PAUSE="splice.txn.committing.pauseTimeMs";
		@SpliceConstants.DefaultValue(COMMITTING_PAUSE) public static final int DEFAULT_COMMITTING_PAUSE=1000;
		public static int committingPause;

		public static void setParameters(Configuration config){
				committingPause = config.getInt(COMMITTING_PAUSE,DEFAULT_COMMITTING_PAUSE);
				transactionTimeout = config.getInt(TRANSACTION_TIMEOUT,DEFAULT_TRANSACTION_TIMEOUT);
				transactionKeepAliveInterval = config.getInt(TRANSACTION_KEEP_ALIVE_INTERVAL,DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL);
		}
}
