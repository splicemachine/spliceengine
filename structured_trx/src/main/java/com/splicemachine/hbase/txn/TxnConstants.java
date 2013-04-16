package com.splicemachine.hbase.txn;

import com.splicemachine.constants.TransactionConstants;
import org.apache.hadoop.hbase.util.Bytes;

public class TxnConstants extends TransactionConstants {
	public static final String TRANSACTION_ID = "__txn_id";
    public static final byte[] TRANSACTION_QUALIFIER = TRANSACTION_ID.getBytes();
    public static final String LOCK_TYPE = "__lock_type";
    public static final String TRANSACTION_ISOLATION_LEVEL = "hbase.transaction.isolationLevel";
    public static final String TRANSACTION_LOCK_TIMEOUT_ATTRIBUTE = "hbase.transaction.lock.timeout";
    public static final String TRANSACTION_LOG_TABLE = "__TXN_LOG";
    public static final byte[] TRANSACTION_LOG_TABLE_BYTES = Bytes.toBytes(TRANSACTION_LOG_TABLE);
    public static final byte[] TRANSACTION_TABLE_PREPARE_FAMILY_BYTES = TxnManagerOperation.PREPARE.toString().getBytes();
    public static final byte[] TRANSACTION_TABLE_DO_FAMILY_BYTES = TxnManagerOperation.DO.toString().getBytes();
    public static final byte[] TRANSACTION_TABLE_ABORT_FAMILY_BYTES = TxnManagerOperation.ABORT.toString().getBytes();
    public static final String INITIALIZE_TRANSACTION_ID = "INITIALIZE_ID__";
    public static final byte[] INITIALIZE_TRANSACTION_ID_BYTES = INITIALIZE_TRANSACTION_ID.getBytes();
    public static final int LATCH_TIMEOUT = 20;
    public static final long TRANSACTION_LOCK_TIMEOUT = 20; //seconds

    public static enum TxnManagerOperation {
		PREPARE,
		DO,
		ABORT
	}
	
	public static enum TransactionIsolationLevel {
		READ_UNCOMMITED, // dirty reads, non-repeatable reads and phantom reads can occur
		READ_COMMITTED, // dirty reads are prevented, non-repeatable reads and phantom reads can occur
		REPEATABLE_READ, // dirty reads and non-repeatable reads are prevented; phantom reads can occur
		SERIALIZABLE // direct reads, non-repeatable reads and phantom reads are prevented
    }

    public static enum Vote {
        YES,
        NO,
        PENDING
    }

}
