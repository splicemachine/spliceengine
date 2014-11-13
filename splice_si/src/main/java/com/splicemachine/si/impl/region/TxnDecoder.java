package com.splicemachine.si.impl.region;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.List;

/**
 * Interface for different mechanisms for encoding/decoding
 * transactions from the transaction table storage.
 *
 * This is an interface so that we can support both the
 * old (non-packed) table format and the new format
 * simultaneously.
 *
 * @author Scott Fines
 * Date: 8/14/14
 */
public abstract class TxnDecoder<TxnInfo,Transaction,Data,Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> {

    abstract Transaction decode(SDataLib<Data,Put,Delete,Get, Scan> datalib, long txnId, Result result) throws IOException;

    public abstract Transaction decode(SDataLib<Data,Put,Delete,Get, Scan> datalib, List<Data> keyValues) throws IOException;

    private static final long TRANSACTION_TIMEOUT_WINDOW = SIConstants.transactionTimeout+1000;
    protected static <Data> Txn.State adjustStateForTimeout(SDataLib dataLib, Txn.State currentState,Data columnLatest,long currTime,boolean oldForm) {
        long lastKATime = decodeKeepAlive(dataLib, columnLatest, oldForm);

        if((currTime-lastKATime)>TRANSACTION_TIMEOUT_WINDOW)
            return Txn.State.ROLLEDBACK; //time out the txn

        return currentState;
    }

    protected static <Data> long decodeKeepAlive(SDataLib dataLib, Data columnLatest, boolean oldForm) {
        long lastKATime;
        if(oldForm){
            /*
             * The old version would put an empty value into the Keep Alive column. If the transaction
             * committed before the keep alive was initiated, then the field will still be null.
             *
             * Since we only read transactions in the old form, and don't create new ones, we just have to decide
             * what to do with these situations. They can only arise if the transaction either
             *
             * A) commits/rolls back before the keep alive can be initiated
             * B) fails before the first keep alive.
             *
             * In the case of a commit/roll back, the value of the keep alive doesn't matter, and in the case
             * of B), we want to fail it. The easiest way to deal with this is just to return 0l.
             */
            int length = dataLib.getDataValuelength(columnLatest);
            if(length==0) return 0l;
            else
                lastKATime = Bytes.toLong(dataLib.getDataValueBuffer(columnLatest), dataLib.getDataValueOffset(columnLatest), length);
        }else
            lastKATime = Encoding.decodeLong(dataLib.getDataValueBuffer(columnLatest), dataLib.getDataValueOffset(columnLatest), false);
        return lastKATime;
    }

    protected static <Data> Txn.State adjustStateForTimeout(SDataLib dataLib, Txn.State currentState,Data columnLatest,boolean oldForm) {
        return adjustStateForTimeout(dataLib,currentState,columnLatest,System.currentTimeMillis(),oldForm);
    }

	public abstract org.apache.hadoop.hbase.client.Put encodeForPut(TxnInfo txn) throws IOException;
        
    protected abstract Transaction composeValue(Data destinationTables, IsolationLevel level, long txnId, long beginTs,long parentTs,  boolean hasAdditive,
    		boolean additive, long commitTs, long globalCommitTs, Txn.State state, long kaTime);
    
}
