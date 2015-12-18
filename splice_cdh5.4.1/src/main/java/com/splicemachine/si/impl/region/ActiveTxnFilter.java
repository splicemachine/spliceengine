package com.splicemachine.si.impl.region;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class ActiveTxnFilter extends FilterBase implements Writable{
    private final SDataLib<OperationWithAttributes,Cell,Delete, Get, Put,RegionScanner,Result,Scan> datalib;
    private final RegionTxnStore txnStore;
    protected final long beforeTs;
    protected final long afterTs;
    private final byte[] destinationTable;
    private final byte[] newEncodedDestinationTable;
    private boolean isAlive = false;
    private boolean stateSeen= false;
    private boolean keepAliveSeen = false;
    private boolean destTablesSeen = false;
    private boolean filter = false;
    private boolean isChild = false;
    private boolean committed = false;
    private MultiFieldDecoder fieldDecoder;
    
    public ActiveTxnFilter(SDataLib<OperationWithAttributes, Cell, Delete, Get, Put, RegionScanner, Result, Scan> datalib,
                           RegionTxnStore txnStore,
                           long beforeTs,
                           long afterTs,
                           byte[] destinationTable) {
        this.txnStore = txnStore;
        this.datalib=datalib;
        this.beforeTs = beforeTs;
        this.afterTs = afterTs;
        this.destinationTable = destinationTable;
        if(destinationTable!=null)
            this.newEncodedDestinationTable = Encoding.encodeBytesUnsorted(destinationTable);
        else
            this.newEncodedDestinationTable = null;
    }

    @Override
    public boolean filterRow() {
        if(filter) return true;
        else if(!isChild && committed) return true; //not an active parent
        else if(destinationTable!=null && !destTablesSeen) return true; //txn didn't modify the table
        else return false;
    }

    @Override
    public void reset() {
        isAlive=false;
        stateSeen=false;
        keepAliveSeen = false;
        destTablesSeen = false;
        filter = false;
        isChild=false;
        committed = false;
    }


    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) {
		/*
		 * Since the transaction id must necessarily be the begin timestamp in both
		 * the old and new format, we can filter transactions out based entirely on the
		 * row key here
		 */
        long txnId = TxnUtils.txnIdFromRowKey(buffer, offset, length);
        boolean withinRange = txnId >= afterTs && txnId <= beforeTs;
        return !withinRange;
    }

    @Override
    public ReturnCode filterKeyValue(Cell kv) throws IOException{
        ReturnCode rCode = filterState(kv);
        if(rCode!=ReturnCode.INCLUDE) return rCode;
        rCode = filterKeepAlive(kv);
        if(rCode!=ReturnCode.INCLUDE) return rCode;
        rCode = filterDestinationTable(kv);
        if(rCode!=ReturnCode.INCLUDE) return rCode;
        rCode = filterCommitTimestamp(kv);
        if(rCode!=ReturnCode.INCLUDE) return rCode;

        detectChild(kv);
        if(rCode!=ReturnCode.INCLUDE) return rCode;
        return ReturnCode.INCLUDE;
    }

    /*
     * The ActiveTxnFilter isn't serialized, so we don't really need to worry about it. Just in case,
     * though, we implement it here
     */
    @Override public void write(DataOutput out) throws IOException{ }
    @Override public void readFields(DataInput in) throws IOException{ }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private ReturnCode filterCommitTimestamp(Cell kv) {
        if(datalib.singleMatchingQualifier(kv,V2TxnDecoder.GLOBAL_COMMIT_QUALIFIER_BYTES)) {
            /*
             * We have a global commit timestamp set. This means that not only are we
             * committed, but we are also (probably) the child of a committed transaction.
             * Thus, we can immediately filter this row out
             */
            committed=true;
            filter = true;
            return ReturnCode.NEXT_ROW;
        }
        //check for a normal commit column
        if(datalib.singleMatchingQualifier(kv,V2TxnDecoder.COMMIT_QUALIFIER_BYTES)){
                /*
                 * We have a commit column, so we've been committed. However, we could
                 * be a child, so we can't filter immediately here
                 */
            committed = true;
        }
        return ReturnCode.INCLUDE;
    }

    private void detectChild(Cell kv) {
        if(datalib.singleMatchingQualifier(kv,V2TxnDecoder.DATA_QUALIFIER_BYTES)){
                /*
                 * We want to pick out the parent transaction id in the data, which is the second
                 * field. To do that, we wrap up in a re-used MultiFieldDecoder, skip the first entry,
                 * then read the second
                 */
            if(fieldDecoder==null)
                fieldDecoder = MultiFieldDecoder.create();            
            fieldDecoder.set(kv.getValueArray(),kv.getValueOffset(),kv.getValueLength());
                        
            
            fieldDecoder.skipLong();
            long pTxnId = fieldDecoder.decodeNextLong();
            isChild = pTxnId>0;            
        }
    }


    private ReturnCode filterDestinationTable(Cell kv) {
        if(destinationTable==null)
            return ReturnCode.INCLUDE; //no destination table to check
        byte[] compareBytes = null;        
        if(datalib.singleMatchingQualifier(kv,V2TxnDecoder.DESTINATION_TABLE_QUALIFIER_BYTES)){
            compareBytes = newEncodedDestinationTable;
        }

        if(compareBytes!=null){
            if(destTablesSeen) return ReturnCode.SKIP;
            destTablesSeen = true;
            if(!Bytes.equals(compareBytes, 0, compareBytes.length,
                    kv.getValueArray(),kv.getValueOffset(),kv.getValueLength())){

                //tables do not match
                filter = true;
                return ReturnCode.NEXT_ROW;
            }
        }
        return ReturnCode.INCLUDE;
    }

    private ReturnCode filterKeepAlive(Cell kv) {
        Txn.State adjustedState;
        if(datalib.singleMatchingQualifier(kv,V2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES)){
            if(keepAliveSeen)
                return ReturnCode.SKIP;
            adjustedState = txnStore.adjustStateForTimeout(Txn.State.ACTIVE,kv);
        } else{
            return ReturnCode.INCLUDE; //not the keep alive column
        }
        keepAliveSeen = true;
        if(adjustedState!= Txn.State.ACTIVE){
                /*
                 * We've timed out. This COULD mean that we're actually timed out, but only
                 * if the transaction claims to still be active. In fact, we have to do a bit better
                 * and make sure that the transaction hasn't been committed/rolledback
                 */
            isAlive = false;
            if(stateSeen &&!committed){
                filter=true;
                return ReturnCode.NEXT_ROW;
            }else
                return ReturnCode.INCLUDE;
        }else{
            isAlive = true;
            return ReturnCode.INCLUDE;
        }
    }

    private ReturnCode filterState(Cell kv) {
        if(committed){
                /*
                 * We've already seen the commit timestamp field, so we know that our state
                 * is committed, no need to check this, just throw it away
                 */
            return ReturnCode.INCLUDE;
        }        
        assert V2TxnDecoder.STATE_QUALIFIER_BYTES.length == 1;
        if(datalib.singleMatchingQualifier(kv,V2TxnDecoder.STATE_QUALIFIER_BYTES)){
            if(stateSeen) return ReturnCode.INCLUDE;
            stateSeen = true;
            byte[] checkState = Txn.State.ACTIVE.encode(); //doesn't actually create a new byte[]
            boolean isActive = Bytes.equals(checkState,0,checkState.length,
                    kv.getValueArray(),kv.getValueOffset(),kv.getValueLength());

                /*
                 * Just because a transaction is active doesn't mean that it's actually active--it could
                 * have been rolled back via the transaction timeout mechanism. Thus, we'll need to check to
                 * make sure that the transaction hasn't timed out while we were working on it.
                 */
            if (isActive && keepAliveSeen && !isAlive) {
                //transaction timed out, so filter it
                filter = true;
                return ReturnCode.NEXT_ROW;
            }
            if(!isActive){
                checkState = Txn.State.ROLLEDBACK.encode();
                if(Bytes.equals(checkState,0,checkState.length,
                        kv.getValueArray(),kv.getValueOffset(),kv.getValueLength())){
                    //we are rolled back, don't need to expose it
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                committed = true; //we know we are committed
            }
            return ReturnCode.INCLUDE;
        }else{
            return ReturnCode.INCLUDE; //we can't filter it based on state
        }
    }

}
