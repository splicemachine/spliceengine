package com.splicemachine.si.impl.region;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public abstract class AbstractV1TxnDecoder<Transaction,Data,Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> extends TxnDecoder<Transaction,Data,Put,Delete,Get,Scan>{
    public static final byte[] FAMILY = SIConstants.DEFAULT_FAMILY_BYTES;

    public static final byte[] OLD_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    public static final byte[] OLD_START_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN);
    public static final byte[] OLD_PARENT_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_PARENT_COLUMN);
    public static final byte[] OLD_DEPENDENT_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_DEPENDENT_COLUMN);
    public static final byte[] OLD_ALLOW_WRITES_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN);
    public static final byte[] OLD_ADDITIVE_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_ADDITIVE_COLUMN);
    public static final byte[] OLD_READ_UNCOMMITTED_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN);
    public static final byte[] OLD_READ_COMMITTED_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_READ_COMMITTED_COLUMN);
    public static final byte[] OLD_KEEP_ALIVE_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN);
    public static final byte[] OLD_STATUS_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    public static final byte[] OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    public static final byte[] OLD_COUNTER_COLUMN = Bytes.toBytes(SIConstants.TRANSACTION_COUNTER_COLUMN);
    public static final byte[] OLD_WRITE_TABLE_COLUMN = Bytes.toBytes(SIConstants.WRITE_TABLE_COLUMN);

    protected AbstractV1TxnDecoder() { }

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
    public Transaction decode(SDataLib<Data,Put,Delete,Get,Scan> dataLib, long txnId,Result result) throws IOException {
    	return decodeInternal(dataLib, txnId, dataLib.getColumnLatest(result,FAMILY,OLD_START_TIMESTAMP_COLUMN),
    			dataLib.getColumnLatest(result,FAMILY,OLD_PARENT_COLUMN),
    			dataLib.getColumnLatest(result,FAMILY, OLD_READ_UNCOMMITTED_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY,OLD_READ_COMMITTED_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY, OLD_STATUS_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY,OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY, OLD_KEEP_ALIVE_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY,OLD_WRITE_TABLE_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY, OLD_DEPENDENT_COLUMN),
        		dataLib.getColumnLatest(result,FAMILY,OLD_ADDITIVE_COLUMN));

    }

    @Override
    public Transaction decode(SDataLib<Data,Put,Delete,Get,Scan> dataLib, List<Data> keyValues) throws IOException {
        if(keyValues.size()<=0) return null;
        Data beginTsVal = null;
        Data parentTxnVal = null;
        Data ruVal = null;
        Data rcVal = null;
        Data statusVal = null;
        Data cTsVal = null;
        Data gTsVal = null;
        Data kaTimeKv = null;
        Data destinationTables = null;
        Data dependentKv = null;
        Data additiveKv = null;
        for(Data kv:keyValues){
        	if (dataLib.singleMatchingColumn(kv, FAMILY, OLD_START_TIMESTAMP_COLUMN))
                beginTsVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_PARENT_COLUMN))
                parentTxnVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_READ_UNCOMMITTED_COLUMN))
                ruVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_READ_COMMITTED_COLUMN))
                rcVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_STATUS_COLUMN))
                statusVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_COMMIT_TIMESTAMP_COLUMN))
                cTsVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_GLOBAL_COMMIT_TIMESTAMP_COLUMN))
                gTsVal = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_KEEP_ALIVE_COLUMN))
                kaTimeKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_WRITE_TABLE_COLUMN))
                destinationTables = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_DEPENDENT_COLUMN))
                dependentKv = kv;
            else if(dataLib.singleMatchingColumn(kv,FAMILY,OLD_ADDITIVE_COLUMN))
                additiveKv = kv;
        }
        if(beginTsVal==null) return null;        
        long txnId = TxnUtils.txnIdFromRowKey(dataLib.getDataRowBuffer(beginTsVal), dataLib.getDataRowOffset(beginTsVal), dataLib.getDataRowlength(beginTsVal));
        return decodeInternal(dataLib,txnId,beginTsVal,parentTxnVal,dependentKv,additiveKv,ruVal,rcVal,statusVal,cTsVal,gTsVal,kaTimeKv,destinationTables);
    }
    
    protected long toLong(SDataLib<Data,Put,Delete,Get,Scan> dataLib, Data data) {
    	return Bytes.toLong(dataLib.getDataValueBuffer(data), dataLib.getDataValueOffset(data), 
        		dataLib.getDataValuelength(data));
    }

    protected boolean toBoolean(SDataLib<Data,Put,Delete,Get,Scan> dataLib, Data data) {
    	return BytesUtil.toBoolean(dataLib.getDataValueBuffer(data), dataLib.getDataValueOffset(data));
    }

	@Override
	public org.apache.hadoop.hbase.client.Put encodeForPut(Transaction txn) throws IOException {
		throw new RuntimeException("Not Implemented");
	}

    protected Transaction decodeInternal(SDataLib<Data,Put,Delete,Get,Scan> dataLib,
    								  long txnId,
    								  Data beginTsVal,
    								  Data parentTxnVal,
    								  Data dependentKv,
    								  Data additiveKv,
    								  Data readUncommittedKv,
    								  Data readCommittedKv,
    								  Data statusKv,
    								  Data commitTimestampKv,
    								  Data globalTimestampKv,
    								  Data keepAliveTimeKv,
    								  Data destinationTables) {
        long beginTs = toLong(dataLib,beginTsVal);
        long parentTs = -1l; //by default, the "root" transaction id is -1l
        if(parentTxnVal!=null)
            parentTs = toLong(dataLib,parentTxnVal); 
        boolean dependent = false;
        boolean hasDependent = false;
        if(dependentKv!=null){
            hasDependent = true;
            dependent = toBoolean(dataLib,dependentKv);
        }
        boolean additive = false;
        boolean hasAdditive = false;
        if(additiveKv!=null){
            additive = toBoolean(dataLib,additiveKv);
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
            readUncommitted = toBoolean(dataLib,readUncommittedKv);
        boolean readCommitted = false;
        if(readCommittedKv!=null)
            readCommitted = toBoolean(dataLib,readCommittedKv);
        Txn.State state = Txn.State.decode(dataLib.getDataValueBuffer(statusKv),dataLib.getDataValueOffset(statusKv),dataLib.getDataValuelength(statusKv));
        long commitTs = -1l;
        if(commitTimestampKv!=null)
            commitTs = toLong(dataLib,commitTimestampKv);
        long globalCommitTs = -1l;
        if(globalTimestampKv!=null)
            globalCommitTs = toLong(dataLib,globalTimestampKv);
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
            state = adjustStateForTimeout(dataLib,state, keepAliveTimeKv,true);
        }
        long kaTime = decodeKeepAlive(dataLib,keepAliveTimeKv,true);

        Txn.IsolationLevel level = null;
        if(readCommittedKv!=null){
            if(readCommitted) level = Txn.IsolationLevel.READ_COMMITTED;
            else if(readUncommittedKv!=null && readUncommitted) level = Txn.IsolationLevel.READ_UNCOMMITTED;
        }else if(readUncommittedKv!=null && readUncommitted)
            level = Txn.IsolationLevel.READ_COMMITTED;

        return composeValue(destinationTables,level,txnId, beginTs,parentTs,hasAdditive,
        		additive,commitTs,globalCommitTs,state,kaTime);
        
    }
}
