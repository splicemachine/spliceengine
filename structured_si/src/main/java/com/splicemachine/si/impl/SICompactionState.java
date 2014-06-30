package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.SpliceReusableHashmap;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.data.api.SDataLib;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
    private final TxnAccess transactionStore;
    private SpliceReusableHashmap<ByteBuffer,KeyValue> evaluatedTransactions;
    public SortedSet<KeyValue> dataToReturn;

		public SICompactionState(SDataLib dataLib, DataStore dataStore, TxnAccess transactionStore) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = new ActiveTxnCacheAccess(transactionStore,SIConstants.activeTransactionCacheSize); //cache active transactions during our scan
        this.dataToReturn  = new TreeSet<KeyValue>(new Comparator<KeyValue>(){
    		@Override
    		public int compare(KeyValue first, KeyValue second) {
    			return KeyValue.COMPARATOR.compare(first, second);
    		}
    	});
        this.evaluatedTransactions = new SpliceReusableHashmap<ByteBuffer,KeyValue>();
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
	 * @param rawList - the input of key values to process
	 * @param results - the output key values
	 */
    public void mutate(List<KeyValue> rawList, List<KeyValue> results) throws IOException {
    	evaluatedTransactions.reset();
    	dataToReturn.clear();
    	for (int i = 0; i< rawList.size(); i++) {
    		mutate(rawList.get(i));
        }
    	results.addAll(dataToReturn);
    }
    
    /**
     * Apply SI mutation logic to an individual key-value. Return the "new" key-value.
     */
    private void mutate(KeyValue keyValue) throws IOException {
        final KeyValueType keyValueType = dataStore.getKeyValueType(keyValue);
    	ByteBuffer buffer = ByteBuffer.wrap(keyValue.getBuffer(),keyValue.getTimestampOffset(),KeyValue.TIMESTAMP_SIZE);
        switch (keyValueType) {
    		case COMMIT_TIMESTAMP:
    			evaluatedTransactions.add(buffer, keyValue);
            	if (isFailedCommitTimestamp(keyValue)) {
            		// No Op KeyValue Lost...
            	} else {
            		dataToReturn.add(keyValue); // log(n): Hopefully not too painful here...  Need sort order
            	}
            	return;
        	case TOMBSTONE:
        	case ANTI_TOMBSTONE:
        		mutateCommitTimestamp(buffer,keyValue);
        		return;
        	case USER_DATA:
        		mutateCommitTimestamp(buffer,keyValue);
        		return;
		default:
			throw new RuntimeException("Saw a non-splice key value");
        }
       }   

    /**
     * Replace unknown commit timestamps with actual commit times.
     */
    private void mutateCommitTimestamp(ByteBuffer buffer, KeyValue keyValue) throws IOException {
    		if (evaluatedTransactions.contains(buffer)) {
				if (isFailedCommitTimestamp(evaluatedTransactions.get(buffer))) {
					return;// No Op failed a commit
				} else {
					dataToReturn.add(keyValue);
					return;
				}	
			}
			Txn transaction = getFromCache(keyValue.getTimestamp());
//			final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
			if (transaction.getState().isFinal()) {
				if (transaction.getState()== Txn.State.COMMITTED) {
					final Long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
					dataToReturn.add(newTransactionTimeStampKeyValue(keyValue,dataLib.encode(globalCommitTimestamp)));
					dataToReturn.add(keyValue);   
				} else {
   				// Finished and not-committed ?? = ROLLEDBACK TODO -sf- implement rollback removal
				}
			} else {
				dataToReturn.add(keyValue);
			}
    }

    private Txn getFromCache(long timestamp) throws IOException {
        Txn result = transactionStore.getTransaction(timestamp);
        return result;
    }

	public static KeyValue newTransactionTimeStampKeyValue(KeyValue keyValue, byte[] value) {
		return new KeyValue(keyValue.getBuffer(),keyValue.getRowOffset(),keyValue.getRowLength(),SIConstants.DEFAULT_FAMILY_BYTES,0,1,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,keyValue.getTimestamp(), KeyValue.Type.Put,value,0,value==null ? 0 : value.length);
	}
	
	public void close() {
		evaluatedTransactions.close();
	}
 
    public static boolean isFailedCommitTimestamp(KeyValue keyValue) {
    	return keyValue.getValueLength() == 1 && keyValue.getBuffer()[keyValue.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }
    
	public static int compareKeyValuesByTimestamp(KeyValue first, KeyValue second) {
		return Bytes.compareTo(first.getBuffer(), first.getTimestampOffset(), KeyValue.TIMESTAMP_SIZE, 
				second.getBuffer(), second.getTimestampOffset(), KeyValue.TIMESTAMP_SIZE);
	}
	public static int compareKeyValuesByColumnAndTimestamp(KeyValue first, KeyValue second) {
		int compare = Bytes.compareTo(first.getBuffer(), first.getQualifierOffset(), first.getQualifierLength(), 
				second.getBuffer(), second.getQualifierOffset(), second.getQualifierLength());
		if (compare != 0)
			return compare;
		return compareKeyValuesByTimestamp(first,second);
	}
    
}
