package com.splicemachine.si.impl.region;

/**
 * Decoder which decodes Transactions stored in the
 * @author Scott Fines
 * Date: 8/18/14
 */

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.TxnUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;

public class V2TxnDecoder<Put extends OperationWithAttributes,Delete,Get extends OperationWithAttributes, Scan> extends AbstractV2TxnDecoder<TxnMessage.Txn,Cell,Put,Delete,Get,Scan>{
	public static final STransactionLib<TxnMessage.Txn,ByteString> transactionLib = SIFactoryDriver.siFactory.getTransactionLib();
    public static final V2TxnDecoder INSTANCE = new V2TxnDecoder();

    private V2TxnDecoder() { 
    	super();
    }
    
	/*
	 * Encodes transaction objects using the new, packed Encoding format
	 *
	 * The new way is a (more) compact representation which uses the Values CF (V) and compact qualifiers (using
	 * the Encoding.encodeX() methods) as follows:
	 *
	 * "d"	--	packed tuple of (beginTimestamp,parentTxnId,isDependent,additive,isolationLevel)
	 * "c"	--	counter (using a packed integer representation)
	 * "k"	--	keepAlive timestamp
	 * "t"	--	commit timestamp
	 * "g"	--	globalCommitTimestamp
	 * "s"	--	state
	 *
	 * The additional columns are kept separate so that they may be updated(and read) independently without
	 * reading and decoding the entire transaction.
	 *
	 * In the new format, if a transaction has been written to the table, then it automatically allows writes
	 *
	 * order: c,d,e,g,k,s,t
	 * order: counter,data,destinationTable,globalCommitTimestamp,keepAlive,state,commitTimestamp,
	 */
@Override
	public org.apache.hadoop.hbase.client.Put encodeForPut(TxnMessage.Txn txn) throws IOException {
		org.apache.hadoop.hbase.client.Put put = new org.apache.hadoop.hbase.client.Put(TxnUtils.getRowKey(transactionLib.getTxnId(txn)));
		MultiFieldEncoder metaFieldEncoder = MultiFieldEncoder.create(5);
		metaFieldEncoder.encodeNext(transactionLib.getBeginTimestamp(txn)).encodeNext(transactionLib.getParentTxnId(txn));

		if(transactionLib.hasAddiditiveField(txn))
			metaFieldEncoder.encodeNext(transactionLib.isAddiditive(txn));
		else
			metaFieldEncoder.encodeEmpty();
		
		Txn.IsolationLevel level = transactionLib.getIsolationLevel(txn);
		if(level!=null)
			metaFieldEncoder.encodeNext(level.encode());
		else 
			metaFieldEncoder.encodeEmpty();
		
		Txn.State state = transactionLib.getTransactionState(txn);
		
		put.add(FAMILY,DATA_QUALIFIER_BYTES,metaFieldEncoder.build());
		put.add(FAMILY,COUNTER_QUALIFIER_BYTES, Encoding.encode(0l));
		put.add(FAMILY,KEEP_ALIVE_QUALIFIER_BYTES,Encoding.encode(System.currentTimeMillis()));
		put.add(FAMILY,STATE_QUALIFIER_BYTES,state.encode());
		if(state== Txn.State.COMMITTED){
			put.add(FAMILY,COMMIT_QUALIFIER_BYTES,Encoding.encode(transactionLib.getCommitTimestamp(txn)));
			long globalCommitTs = transactionLib.getGlobalCommitTimestamp(txn);
			if(globalCommitTs>=0)
				put.add(FAMILY,GLOBAL_COMMIT_QUALIFIER_BYTES,Encoding.encode(globalCommitTs));
		}
		ByteString destTableBuffer = transactionLib.getDestinationTableBuffer(txn);
		if(destTableBuffer!=null && !destTableBuffer.isEmpty())
		put.add(FAMILY,DESTINATION_TABLE_QUALIFIER_BYTES,destTableBuffer.toByteArray());
		return put;
	}
    
	protected TxnMessage.Txn composeValue(Cell destinationTables, IsolationLevel level, long txnId, long beginTs,long parentTs,  boolean hasAdditive,
		boolean additive, long commitTs, long globalCommitTs, Txn.State state, long kaTime) {
			return TXNDecoderUtils.composeValue(destinationTables, level, txnId, beginTs, parentTs, hasAdditive, additive, commitTs, globalCommitTs, state, kaTime);    	
	}


}
