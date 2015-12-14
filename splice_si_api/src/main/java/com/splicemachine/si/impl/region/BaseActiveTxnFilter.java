package com.splicemachine.si.impl.region;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.SReturnCodeLib;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.TxnUtils;
import com.splicemachine.si.impl.driver.SIDriver;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class BaseActiveTxnFilter<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,Get extends OperationWithAttributes,
        Put extends OperationWithAttributes,RegionScanner,Result,ReturnCode,Scan extends OperationWithAttributes> {
    private SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> datalib = SIDriver.getDataLib();
    private SReturnCodeLib<ReturnCode> returnCodeLib = SIDriver.getReturnCodeLib();
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
    
    public BaseActiveTxnFilter(long beforeTs, long afterTs, byte[] destinationTable) {
        this.beforeTs = beforeTs;
        this.afterTs = afterTs;
        this.destinationTable = destinationTable;
        if(destinationTable!=null)
            this.newEncodedDestinationTable = Encoding.encodeBytesUnsorted(destinationTable);
        else
            this.newEncodedDestinationTable = null;
    }

    public boolean filterRow() {
        if(filter) return true;
        else if(!isChild && committed) return true; //not an active parent
        else if(destinationTable!=null && !destTablesSeen) return true; //txn didn't modify the table
        else return false;
    }

    public void reset() {
        isAlive=false;
        stateSeen=false;
        keepAliveSeen = false;
        destTablesSeen = false;
        filter = false;
        isChild=false;
        committed = false;
    }

    public ReturnCode internalFilter(Data kv) {
        ReturnCode rCode = filterState(kv);
        if(rCode!=returnCodeLib.getIncludeReturnCode()) return rCode;
        rCode = filterKeepAlive(kv);
        if(rCode!=returnCodeLib.getIncludeReturnCode()) return rCode;
        rCode = filterDestinationTable(kv);
        if(rCode!=returnCodeLib.getIncludeReturnCode()) return rCode;
        rCode = filterCommitTimestamp(kv);
        if(rCode!=returnCodeLib.getIncludeReturnCode()) return rCode;

        detectChild(kv);
        return returnCodeLib.getIncludeReturnCode();
    }

    private ReturnCode filterCommitTimestamp(Data kv) {    	
        if(datalib.singleMatchingQualifier(kv,AbstractV2TxnDecoder.GLOBAL_COMMIT_QUALIFIER_BYTES)) {
            /*
             * We have a global commit timestamp set. This means that not only are we
             * committed, but we are also (probably) the child of a committed transaction.
             * Thus, we can immediately filter this row out
             */
            committed=true;
            filter = true;
            return returnCodeLib.getNextRowReturnCode();
        }
        //check for a normal commit column
        if(datalib.singleMatchingQualifier(kv,AbstractV2TxnDecoder.COMMIT_QUALIFIER_BYTES)){
                /*
                 * We have a commit column, so we've been committed. However, we could
                 * be a child, so we can't filter immediately here
                 */
            committed = true;
        }
        return returnCodeLib.getIncludeReturnCode();
    }

    private void detectChild(Data kv) {
        if(datalib.singleMatchingQualifier(kv,AbstractV2TxnDecoder.DATA_QUALIFIER_BYTES)){
                /*
                 * We want to pick out the parent transaction id in the data, which is the second
                 * field. To do that, we wrap up in a re-used MultiFieldDecoder, skip the first entry,
                 * then read the second
                 */
            if(fieldDecoder==null)
                fieldDecoder = MultiFieldDecoder.create();            
            fieldDecoder.set(datalib.getDataValueBuffer(kv),
            		datalib.getDataValueOffset(kv),
            		datalib.getDataValuelength(kv));
                        
            
            fieldDecoder.skipLong();
            long pTxnId = fieldDecoder.decodeNextLong();
            isChild = pTxnId>0;            
        }
    }

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

    private ReturnCode filterDestinationTable(Data kv) {
        if(destinationTable==null)
            return returnCodeLib.getIncludeReturnCode(); //no destination table to check
        byte[] compareBytes = null;        
        if(datalib.singleMatchingQualifier(kv,AbstractV2TxnDecoder.DESTINATION_TABLE_QUALIFIER_BYTES)){
            compareBytes = newEncodedDestinationTable;
        }

        if(compareBytes!=null){
            if(destTablesSeen) return returnCodeLib.getSkipReturnCode();
            destTablesSeen = true;
            if(!Bytes.equals(compareBytes, 0, compareBytes.length,
                    datalib.getDataValueBuffer(kv),
                    datalib.getDataValueOffset(kv),
                    datalib.getDataValuelength(kv))) {
            		
                //tables do not match
                filter = true;
                return returnCodeLib.getNextRowReturnCode();
            }
        }
        return returnCodeLib.getIncludeReturnCode();
    }

    private ReturnCode filterKeepAlive(Data kv) {
        Txn.State adjustedState;
        if(datalib.singleMatchingQualifier(kv,AbstractV2TxnDecoder.KEEP_ALIVE_QUALIFIER_BYTES)){
            if(keepAliveSeen)
                return returnCodeLib.getSkipReturnCode();
            adjustedState = AbstractV2TxnDecoder.adjustStateForTimeout(datalib, Txn.State.ACTIVE, kv, false);
        } else{
            return returnCodeLib.getIncludeReturnCode(); //not the keep alive column
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
                return returnCodeLib.getNextRowReturnCode();
            }else
                return returnCodeLib.getIncludeReturnCode();
        }else{
            isAlive = true;
            return returnCodeLib.getIncludeReturnCode();
        }
    }

    private ReturnCode filterState(Data kv) {
        if(committed){
                /*
                 * We've already seen the commit timestamp field, so we know that our state
                 * is committed, no need to check this, just throw it away
                 */
            return returnCodeLib.getIncludeReturnCode();
        }        
        assert AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES.length == 1;
        if(datalib.singleMatchingQualifier(kv,AbstractV2TxnDecoder.STATE_QUALIFIER_BYTES)){
            if(stateSeen) return returnCodeLib.getSkipReturnCode();
            stateSeen = true;
            byte[] checkState = Txn.State.ACTIVE.encode(); //doesn't actually create a new byte[]
            boolean isActive = Bytes.equals(checkState,0,checkState.length,
            		datalib.getDataValueBuffer(kv),
            		datalib.getDataValueOffset(kv),
            		datalib.getDataValuelength(kv));
            		
                /*
                 * Just because a transaction is active doesn't mean that it's actually active--it could
                 * have been rolled back via the transaction timeout mechanism. Thus, we'll need to check to
                 * make sure that the transaction hasn't timed out while we were working on it.
                 */
            if (isActive && keepAliveSeen && !isAlive) {
                //transaction timed out, so filter it
                filter = true;
                return returnCodeLib.getNextRowReturnCode();
            }
            if(!isActive){
                checkState = Txn.State.ROLLEDBACK.encode();
                if(Bytes.equals(checkState,0,checkState.length,
                		datalib.getDataValueBuffer(kv),
                		datalib.getDataValueOffset(kv),
                		datalib.getDataValuelength(kv))){
                    //we are rolled back, don't need to expose it
                    filter = true;
                    return returnCodeLib.getNextRowReturnCode();
                }
                committed = true; //we know we are committed
            }
            return returnCodeLib.getIncludeReturnCode();
        }else{
            return returnCodeLib.getIncludeReturnCode(); //we can't filter it based on state
        }
    }

}
