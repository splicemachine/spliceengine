package com.splicemachine.si.impl.region;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SparseTxn;
import org.apache.hadoop.hbase.KeyValue;
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
abstract class TxnDecoder {

    abstract SparseTxn decode(long txnId, Result result) throws IOException;

    abstract DenseTxn decode(List<KeyValue> keyValues) throws IOException;

    private static final long TRANSACTION_TIMEOUT_WINDOW = SIConstants.transactionTimeout+1000;
    protected static Txn.State adjustStateForTimeout(Txn.State currentState,KeyValue columnLatest,long currTime,boolean oldForm) {
        long lastKATime = decodeKeepAlive(columnLatest, oldForm);

        if((currTime-lastKATime)>TRANSACTION_TIMEOUT_WINDOW)
            return Txn.State.ROLLEDBACK; //time out the txn

        return currentState;
    }

    protected static long decodeKeepAlive(KeyValue columnLatest, boolean oldForm) {
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
            int length = columnLatest.getValueLength();
            if(length==0) return 0l;
            else
                lastKATime = Bytes.toLong(columnLatest.getBuffer(), columnLatest.getValueOffset(), length);
        }else
            lastKATime = Encoding.decodeLong(columnLatest.getBuffer(), columnLatest.getValueOffset(), false);
        return lastKATime;
    }

    protected static Txn.State adjustStateForTimeout(Txn.State currentState,KeyValue columnLatest,boolean oldForm) {
        return adjustStateForTimeout(currentState,columnLatest,System.currentTimeMillis(),oldForm);
    }
}
