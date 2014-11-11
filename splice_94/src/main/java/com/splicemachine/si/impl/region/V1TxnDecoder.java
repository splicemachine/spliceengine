package com.splicemachine.si.impl.region;

import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.Txn.State;
import com.splicemachine.si.impl.DenseTxn;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

/**
 * @author Scott Fines
 *         Date: 8/18/14
 */
public class V1TxnDecoder<Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> extends AbstractV1TxnDecoder<DenseTxn,KeyValue,Put,Delete,Get,Scan>{
    public static final V1TxnDecoder INSTANCE = new V1TxnDecoder();

    private V1TxnDecoder() { 
    	super();
    }

	@Override
	protected DenseTxn composeValue(KeyValue destinationTables,
			IsolationLevel level, long txnId, long beginTs, long parentTs,
			boolean hasAdditive, boolean additive, long commitTs,
			long globalCommitTs, State state, long kaTime) {
		return TXNDecoderUtils.composeValue(destinationTables, level, txnId, beginTs, parentTs, hasAdditive, additive, commitTs, globalCommitTs, state, kaTime);
	}
}
