package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.coprocessor.TxnMessage;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class V1TxnDecoder<Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> extends AbstractV1TxnDecoder<TxnMessage.Txn,Cell,Put,Delete,Get,Scan>{
    public static final V1TxnDecoder INSTANCE = new V1TxnDecoder();
    private V1TxnDecoder() { 
    	super();
    }
    protected TxnMessage.Txn composeValue(Cell destinationTables, IsolationLevel level, long txnId, long beginTs,long parentTs,  boolean hasAdditive,
    		boolean additive, long commitTs, long globalCommitTs, Txn.State state, long kaTime) {
    	return TXNDecoderUtils.composeValue(destinationTables, level, txnId, beginTs, parentTs, hasAdditive, additive, commitTs, globalCommitTs, state, kaTime);    	
    }

}
