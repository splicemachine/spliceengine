package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.SpliceReusableHashmap;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Captures the SI logic to perform when a data table is compacted (without explicit HBase dependencies). Provides the
 * guts for SICompactionScanner.
 * <p/>
 * It is handed key-values and can change them.
 */
public class SICompactionState<Result,  Mutation,
        Put extends OperationWithAttributes, Delete, Get extends OperationWithAttributes, Scan, IHTable, Lock, OperationStatus> {
    private final SDataLib<Put, Delete, Get, Scan> dataLib;
    private final DataStore<Mutation, Put, Delete, Get, Scan, IHTable> dataStore;
    private final TransactionStore transactionStore;
    private SpliceReusableHashmap<ByteBuffer, KeyValue> evaluatedTransactions;

    /**
     * Cache of transactions that have been read during the execution of this compaction.
     */
    private final Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    public SICompactionState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        evaluatedTransactions = new SpliceReusableHashmap<ByteBuffer, KeyValue>();
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
		 * @param rawList - the input of key values to process
		 * @param results - the output key values
		 */
    public void mutate(List<KeyValue> rawList, List<KeyValue> results) throws IOException {
    	evaluatedTransactions.reset();
    	for (int i = 0; i< rawList.size(); i++) {
    		mutate(results, rawList.get(i));
        }
    }
    
    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private void mutate(List<KeyValue> results, KeyValue keyValue) throws IOException {
        final KeyValueType keyValueType = dataStore.getKeyValueType(keyValue);
    	ByteBuffer buffer = ByteBuffer.wrap(keyValue.getBuffer(),keyValue.getTimestampOffset(),KeyValue.TIMESTAMP_SIZE);
        if (keyValueType.equals(KeyValueType.COMMIT_TIMESTAMP)) {
        	if (keyValue.getValueLength() == 1 && keyValue.getBuffer()[keyValue.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0]) {
        		// remove Failed Timestamp
        	} else {
        		if (!evaluatedTransactions.contains(buffer)) // remove duplicates: race condition
        			results.add(keyValue);        		
        	}        		
        	evaluatedTransactions.add(buffer,keyValue);
        } else {
        	KeyValue evalTrans = evaluatedTransactions.get(buffer);
        	if (evalTrans == null) {
        		mutateCommitTimestamp(results, keyValue);
        	} else if (evalTrans.getValueLength() == 1 && evalTrans.getBuffer()[evalTrans.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0]) {
        		// remove VK with failed timestamp
        	} else {
        		results.add(keyValue);
        	}
        }
    }

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private void mutateCommitTimestamp(List<KeyValue> results, KeyValue keyValue) throws IOException {
            final Transaction transaction = getFromCache(keyValue.getTimestamp());
            final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
            if (effectiveStatus.isFinished()) {
                final Long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
                if (effectiveStatus.isCommitted()) {
                    results.add(newTransactionTimeStampKeyValue(keyValue,dataLib.encode(globalCommitTimestamp)));
                    results.add(keyValue);                	
                } else {
                	// ignore si fail data...
                }
        }
    }

    private Transaction getFromCache(long timestamp) throws IOException {
        Transaction result = transactionCache.get(timestamp);
        if (result == null) {
            result = transactionStore.getTransaction(timestamp);
            transactionCache.put(timestamp, result);
        }
        return result;
    }

	public static KeyValue newTransactionTimeStampKeyValue(KeyValue keyValue, byte[] value) {
		return new KeyValue(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength(),SIConstants.DEFAULT_FAMILY_BYTES,0,1,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,keyValue.getTimestamp(), KeyValue.Type.Put,value,0,value==null ? 0 : value.length);
	}
	
	public void close() {
		evaluatedTransactions.close();
	}
    
}
