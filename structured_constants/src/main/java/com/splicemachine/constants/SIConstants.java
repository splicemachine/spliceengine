package com.splicemachine.constants;

import com.splicemachine.encoding.Encoding;

/**
 * Defines the schema used by SI for the transaction table and for additional metadata on data tables.
 */

public class SIConstants extends SpliceConstants {
    static {
        setParameters();
    }

    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final byte[] SNAPSHOT_ISOLATION_FAILED_TIMESTAMP = new byte[] {-1};

    public static final String SNAPSHOT_ISOLATION_FAMILY = "S";
    public static final byte[] SNAPSHOT_ISOLATION_FAMILY_BYTES = SNAPSHOT_ISOLATION_FAMILY.getBytes();
    public static final int TRANSACTION_ID_COLUMN = 14;
    public static final int TRANSACTION_START_TIMESTAMP_COLUMN = 0;
    public static final int TRANSACTION_PARENT_COLUMN = 1;
    public static final int TRANSACTION_DEPENDENT_COLUMN = 2;
    public static final int TRANSACTION_ALLOW_WRITES_COLUMN = 3;
    public static final int TRANSACTION_READ_UNCOMMITTED_COLUMN = 4;
    public static final int TRANSACTION_READ_COMMITTED_COLUMN = 5;
    public static final int TRANSACTION_STATUS_COLUMN = 6;
    public static final int TRANSACTION_COMMIT_TIMESTAMP_COLUMN = 7;
    public static final int TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN = 16;
    public static final int TRANSACTION_KEEP_ALIVE_COLUMN = 8;
    public static final int TRANSACTION_COUNTER_COLUMN = 15;

    public static final byte[] TRANSACTION_ID_COLUMN_BYTES = Encoding.encode(TRANSACTION_ID_COLUMN);
    public static final byte[] TRANSACTION_START_TIMESTAMP_COLUMN_BYTES = Encoding.encode(TRANSACTION_START_TIMESTAMP_COLUMN);
    public static final byte[] TRANSACTION_PARENT_COLUMN_BYTES = Encoding.encode(TRANSACTION_PARENT_COLUMN);
    public static final byte[] TRANSACTION_DEPENDENT_COLUMN_BYTES = Encoding.encode(TRANSACTION_DEPENDENT_COLUMN);
    public static final byte[] TRANSACTION_ALLOW_WRITES_COLUMN_BYTES = Encoding.encode(TRANSACTION_ALLOW_WRITES_COLUMN);
    public static final byte[] TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES = Encoding.encode(TRANSACTION_READ_UNCOMMITTED_COLUMN);
    public static final byte[] TRANSACTION_READ_COMMITTED_COLUMN_BYTES = Encoding.encode(TRANSACTION_READ_COMMITTED_COLUMN);
    public static final byte[] TRANSACTION_STATUS_COLUMN_BYTES = Encoding.encode(TRANSACTION_STATUS_COLUMN);
    public static final byte[] TRANSACTION_COMMIT_TIMESTAMP_COLUMN_BYTES = Encoding.encode(TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    public static final byte[] TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN_BYTES = Encoding.encode(TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    public static final byte[] TRANSACTION_KEEP_ALIVE_COLUMN_BYTES = Encoding.encode(TRANSACTION_KEEP_ALIVE_COLUMN);
    public static final byte[] TRANSACTION_COUNTER_COLUMN_BYTES = Encoding.encode(TRANSACTION_COUNTER_COLUMN);

    public static final byte[] TRANSACTION_FAMILY_BYTES = SpliceConstants.DEFAULT_FAMILY.getBytes();
    public static final int SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN = 0;
    public static final int SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN = 1;
    public static final String SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING = SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN + "";
    public static final String SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING = SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN + "";
    public static final byte[] SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES = Encoding.encode(SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN);

    public static final short SI_NEEDED_VALUE = (short) 0;
    public static final short ONLY_SI_FAMILY_NEEDED_VALUE = (short) 1;

    public static final int TRANSACTION_KEEP_ALIVE_INTERVAL = 1 * 60 * 1000;
    public static final int TRANSACTION_TIMEOUT = 10 * TRANSACTION_KEEP_ALIVE_INTERVAL;
    
    
    public static final String SI_TRANSACTION_ID_KEY = "A";
    public static final String SI_NEEDED = "B";
    public static final String SI_UNCOMMITTED = "C";
    public static final String SI_DELETE_PUT = "D";

    public static final String SI_ANTI_TOMBSTONE_VALUE = "Z";
}
