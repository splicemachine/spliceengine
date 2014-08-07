package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.TransactionStoreDecoder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Uses the original form for the transaction table.
 *
 * @author Scott Fines
 * Date: 7/30/14
 */
public class BasicTransactionStoreDecoder implements TransactionStoreDecoder{
    private static final byte[] TXN_ID_COL = Bytes.toBytes(SIConstants.TRANSACTION_ID_COLUMN);
    private static final byte[] GLOBAL_COMMIT_COL = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] BEGIN_TIMESTAMP = Bytes.toBytes(SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN);
    private static final byte[] STATUS = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    private static final byte[] COMMIT_TIMESTAMP = Bytes.toBytes(SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] COUNTER = Bytes.toBytes(SIConstants.TRANSACTION_COUNTER_COLUMN);
    private static final byte[] PARENT = Bytes.toBytes(SIConstants.TRANSACTION_PARENT_COLUMN);
    private static final byte[] WRITES = Bytes.toBytes(SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN);
    private static final byte[] DEPENDENT = Bytes.toBytes(SIConstants.TRANSACTION_DEPENDENT_COLUMN);
    private static final byte[] UNCOMMITTED = Bytes.toBytes(SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN);
    private static final byte[] COMMITTED = Bytes.toBytes(SIConstants.TRANSACTION_READ_COMMITTED_COLUMN);
    private static final byte[] KEEP_ALIVE = Bytes.toBytes(SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN);
    private static final byte[] WRITE_TABLE = Bytes.toBytes(SIConstants.WRITE_TABLE_COLUMN);
    private static final byte[] ADDITIVE = Bytes.toBytes(SIConstants.TRANSACTION_ADDITIVE_COLUMN);

    private TransactionStore txnStore;

    @Override
    public void initialize() throws IOException {
        txnStore = HTransactorFactory.getTransactionStore();
    }

    @Override
    public Transaction decode(List<KeyValue> keyValues) throws IOException {
        if(keyValues==null||keyValues.size()<=0) return null;

        long id = -1;
        Long globalCommit = null;
        long beginTimestamp = Long.MAX_VALUE;
        TransactionStatus txnStatus = null;
        Long commitTimestamp = null;
        Long counter = null;
        Long parent = null;
        boolean writes = false;
        boolean dependent = false;
        Boolean readUncommitted = null;
        Boolean readCommitted = null;
        long keepAliveValue = 0;
        Boolean additive = null;
        byte[] writeTable = null;

        byte[] family = SpliceConstants.DEFAULT_FAMILY_BYTES;
        for(KeyValue kv:keyValues){
            if(!kv.matchingFamily(family))
                continue;
            if(kv.matchingColumn(family, TXN_ID_COL))
                id = Bytes.toLong(kv.getBuffer(), kv.getValueOffset());
            else if(kv.matchingColumn(family, GLOBAL_COMMIT_COL))
                globalCommit = Bytes.toLong(kv.getBuffer(), kv.getValueOffset());
            else if(kv.matchingColumn(family,BEGIN_TIMESTAMP))
                beginTimestamp = Bytes.toLong(kv.getBuffer(), kv.getValueOffset());
            else if(kv.matchingColumn(family,STATUS))
                txnStatus = TransactionStatus.values()[Bytes.toInt(kv.getBuffer(),kv.getValueOffset())];
            else if(kv.matchingColumn(family, COMMIT_TIMESTAMP))
                commitTimestamp = Bytes.toLong(kv.getBuffer(), kv.getValueOffset());
            else if(kv.matchingColumn(family,COUNTER))
                counter = Bytes.toLong(kv.getBuffer(),kv.getValueOffset());
            else if(kv.matchingColumn(family,PARENT))
                parent = Bytes.toLong(kv.getBuffer(),kv.getValueOffset());
            else if(kv.matchingColumn(family,WRITES))
                writes = BytesUtil.toBoolean(kv.getBuffer(), kv.getValueOffset());
            else if(kv.matchingColumn(family,DEPENDENT))
                dependent = BytesUtil.toBoolean(kv.getBuffer(),kv.getValueOffset());
            else if(kv.matchingColumn(family,UNCOMMITTED))
                readUncommitted = BytesUtil.toBoolean(kv.getBuffer(),kv.getValueOffset());
            else if(kv.matchingColumn(family,COMMITTED))
                readCommitted = BytesUtil.toBoolean(kv.getBuffer(),kv.getValueOffset());
            else if(kv.matchingColumn(family,KEEP_ALIVE))
                keepAliveValue = Bytes.toLong(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
            else if(KeyValueUtils.singleMatchingColumn(kv,family,WRITE_TABLE))
                writeTable = kv.getValue(); //need to make a copy of the entry
            else if(KeyValueUtils.singleMatchingColumn(kv,family,ADDITIVE)){
                additive = BytesUtil.toBoolean(kv.getBuffer(),kv.getValueOffset());
            }
        }

        final TransactionBehavior transactionBehavior = dependent ?
                StubTransactionBehavior.instance :
                IndependentTransactionBehavior.instance;

        Transaction parentTxn = parent==null?null: txnStore.getTransaction(parent);

        return new Transaction(transactionBehavior,
                id,
                beginTimestamp,
                keepAliveValue,
                parentTxn,
                dependent,
                writes,
                additive,
                readUncommitted,
                readCommitted,
                txnStatus,
                commitTimestamp,
                globalCommit,
                counter,
                writeTable);
    }

}
