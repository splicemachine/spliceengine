package com.splicemachine.si.impl.region;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SparseTxn;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class V1TxnDecoder extends TxnDecoder{
    private static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;

    static final byte[] OLD_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    static final byte[] OLD_START_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN);
    static final byte[] OLD_PARENT_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_PARENT_COLUMN);
    static final byte[] OLD_DEPENDENT_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_DEPENDENT_COLUMN);
    static final byte[] OLD_ALLOW_WRITES_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN);
    static final byte[] OLD_ADDITIVE_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_ADDITIVE_COLUMN);
    static final byte[] OLD_READ_UNCOMMITTED_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN);
    static final byte[] OLD_READ_COMMITTED_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_READ_COMMITTED_COLUMN);
    static final byte[] OLD_KEEP_ALIVE_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN);
    static final byte[] OLD_STATUS_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    static final byte[] OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    static final byte[] OLD_COUNTER_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_COUNTER_COLUMN);
    static final byte[] OLD_WRITE_TABLE_COLUMN = Bytes.toBytes(SIConstants.WRITE_TABLE_COLUMN);

    public static final V1TxnDecoder INSTANCE = new V1TxnDecoder();

    private V1TxnDecoder() { }

    /*
           * 1. The old way uses the Values CF (V) and integer qualifiers (Bytes.toBytes(int)) to encode data
           * to the following columns:
           * 	0 	--	beginTimestamp
           *  1 	--	parentTxnId
           *  2 	--	isDependent			(may not be present)
           *  3 	--	allowWrites			(may not be present)
           *  4 	--	readUncommitted	(may not be present)
           *  5 	--	readCommitted		(may not be present)
           *  6		--	status					(either ACTIVE, COMMITTED,ROLLED_BACK, or ERROR)
           *  7		--	commitTimestamp	(may not be present)
           *  8		--	keepAlive
           *	14	--	transaction id
           *	15	--	transaction counter
           *	16	--	globalCommitTimestamp (may not be present)
           *	17	--	additive				(may not be present)
           */
    @Override
    public SparseTxn decode(long txnId,Result result) throws IOException {
        KeyValue beginTsVal = result.getColumnLatest(FAMILY,OLD_START_TIMESTAMP_COLUMN);
        KeyValue parentTxnVal = result.getColumnLatest(FAMILY,OLD_PARENT_COLUMN);
        KeyValue ruVal = result.getColumnLatest(FAMILY, OLD_READ_UNCOMMITTED_COLUMN);
        KeyValue rcVal = result.getColumnLatest(FAMILY,OLD_READ_COMMITTED_COLUMN);
        KeyValue statusVal = result.getColumnLatest(FAMILY, OLD_STATUS_COLUMN);
        KeyValue cTsVal = result.getColumnLatest(FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN);
        KeyValue gTsVal = result.getColumnLatest(FAMILY,OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
        KeyValue kaTimeKv = result.getColumnLatest(FAMILY, OLD_KEEP_ALIVE_COLUMN);
        KeyValue destinationTables = result.getColumnLatest(FAMILY,OLD_WRITE_TABLE_COLUMN);
        KeyValue dependentKv = result.getColumnLatest(FAMILY, OLD_DEPENDENT_COLUMN);
        KeyValue additiveKv = result.getColumnLatest(FAMILY,OLD_ADDITIVE_COLUMN);

        return decodeInternal(txnId, beginTsVal, parentTxnVal, dependentKv,additiveKv,ruVal, rcVal, statusVal, cTsVal, gTsVal, kaTimeKv, destinationTables);
    }

    @Override
    public DenseTxn decode(List<KeyValue> keyValues) throws IOException {
        if(keyValues.size()<=0) return null;
        KeyValue beginTsVal = null;
        KeyValue parentTxnVal = null;
        KeyValue ruVal = null;
        KeyValue rcVal = null;
        KeyValue statusVal = null;
        KeyValue cTsVal = null;
        KeyValue gTsVal = null;
        KeyValue kaTimeKv = null;
        KeyValue destinationTables = null;
        KeyValue dependentKv = null;
        KeyValue additiveKv = null;
        for(KeyValue kv:keyValues){
            if(KeyValueUtils.singleMatchingColumn(kv, FAMILY, OLD_START_TIMESTAMP_COLUMN))
                beginTsVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_PARENT_COLUMN))
                parentTxnVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_READ_UNCOMMITTED_COLUMN))
                ruVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_READ_COMMITTED_COLUMN))
                rcVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_STATUS_COLUMN))
                statusVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN))
                cTsVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN))
                gTsVal = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_KEEP_ALIVE_COLUMN))
                kaTimeKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_WRITE_TABLE_COLUMN))
                destinationTables = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_DEPENDENT_COLUMN))
                dependentKv = kv;
            else if(KeyValueUtils.singleMatchingColumn(kv,FAMILY,OLD_ADDITIVE_COLUMN))
                additiveKv = kv;
        }
        if(beginTsVal==null) return null;
        long txnId = TxnUtils.txnIdFromRowKey(beginTsVal.getBuffer(), beginTsVal.getRowOffset(), beginTsVal.getRowLength());
        return decodeInternal(txnId,beginTsVal,parentTxnVal,dependentKv,additiveKv,ruVal,rcVal,statusVal,cTsVal,gTsVal,kaTimeKv,destinationTables);
    }

    protected DenseTxn decodeInternal(long txnId,
                                      KeyValue beginTsVal,
                                      KeyValue parentTxnVal,
                                      KeyValue dependentKv,
                                      KeyValue additiveKv,
                                      KeyValue readUncommittedKv,
                                      KeyValue readCommittedKv,
                                      KeyValue statusKv,
                                      KeyValue commitTimestampKv,
                                      KeyValue globalTimestampKv,
                                      KeyValue keepAliveTimeKv,
                                      KeyValue destinationTables) {
        long beginTs = Bytes.toLong(beginTsVal.getBuffer(), beginTsVal.getValueOffset(), beginTsVal.getValueLength());
        long parentTs = -1l; //by default, the "root" transaction id is -1l
        if(parentTxnVal!=null)
            parentTs = Bytes.toLong(parentTxnVal.getBuffer(),parentTxnVal.getValueOffset(),parentTxnVal.getValueLength());
        boolean dependent = false;
        boolean hasDependent = false;
        if(dependentKv!=null){
            hasDependent = true;
            dependent = BytesUtil.toBoolean(dependentKv.getBuffer(), dependentKv.getValueOffset());
        }
        boolean additive = false;
        boolean hasAdditive = false;
        if(additiveKv!=null){
            additive = BytesUtil.toBoolean(additiveKv.getBuffer(),additiveKv.getValueOffset());
            hasAdditive = true;
        }
						/*
						 * We can ignore the ALLOW_WRITES field, for the following reason:
						 *
						 * In the new structure, if the transaction does not allow writes, then it won't be recorded
						 * on the table itself. Thus, any new transaction automatically allows writes. However,
						 * we require that a full cluster restart occur (e.g. all nodes must go down, then come back up) in
						 * order for the new transactional system to work. Therefore, all transactions using the old system
						 * will no longer be writing data. Hence, the old transaction encoding format will only ever
						 * be seen when READING from the transaction table. Read only transactions will never be read from the
						 * table again (since they never wrote anything). Hence, we can safely assume that if someone is
						 * interested in this transaction, then they are interested in writable transactions only, and that
						 * this is therefore writable.
						 */
        boolean readUncommitted = false;
        if(readUncommittedKv!=null)
            readUncommitted = BytesUtil.toBoolean(readUncommittedKv.getBuffer(), readUncommittedKv.getValueOffset());
        boolean readCommitted = false;
        if(readCommittedKv!=null)
            readCommitted = BytesUtil.toBoolean(readCommittedKv.getBuffer(), readCommittedKv.getValueOffset());
        Txn.State state = Txn.State.decode(statusKv.getBuffer(), statusKv.getValueOffset(), statusKv.getValueLength());
        long commitTs = -1l;
        if(commitTimestampKv!=null)
            commitTs = Bytes.toLong(commitTimestampKv.getBuffer(), commitTimestampKv.getValueOffset(), commitTimestampKv.getValueLength());
        long globalCommitTs = -1l;
        if(globalTimestampKv!=null)
            globalCommitTs = Bytes.toLong(globalTimestampKv.getBuffer(),globalTimestampKv.getValueOffset(),globalTimestampKv.getValueLength());
        else if(hasDependent &&!dependent){
            //performance enhancement to avoid an extra region get() during commit time
            globalCommitTs = commitTs;
        }

        if(state== Txn.State.ACTIVE){
                /*
                 * We need to check that the transaction hasn't been timed out (and therefore rolled back). This
                 * happens if the keepAliveTime is older than the configured transaction timeout. Of course,
                 * there is some network latency which could cause small keep alives to be problematic. To help out,
                 * we allow a little fudge factor in the timeout
                 */
            state = adjustStateForTimeout(state, keepAliveTimeKv,true);
        }
        long kaTime = decodeKeepAlive(keepAliveTimeKv,true);

        Txn.IsolationLevel level = null;
        if(readCommittedKv!=null){
            if(readCommitted) level = Txn.IsolationLevel.READ_COMMITTED;
            else if(readUncommittedKv!=null && readUncommitted) level = Txn.IsolationLevel.READ_UNCOMMITTED;
        }else if(readUncommittedKv!=null && readUncommitted)
            level = Txn.IsolationLevel.READ_COMMITTED;


        ByteSlice destTableBuffer = null;
        if(destinationTables!=null){
            destTableBuffer = new ByteSlice();
            destTableBuffer.set(destinationTables.getBuffer(),destinationTables.getValueOffset(),destinationTables.getValueLength());
        }

        return new DenseTxn(txnId,beginTs,parentTs,
                commitTs,globalCommitTs, hasAdditive,additive,level,state,destTableBuffer,kaTime);
    }
}
