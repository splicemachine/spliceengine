package com.splicemachine.si.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.si.SpliceReusableHashmap;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.SDataLib;

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
    private SpliceReusableHashmap<ByteBuffer,Cell> evaluatedTransactions;
    public SortedSet<Cell> dataToReturn;

    /**
     * Cache of transactions that have been read during the execution of this compaction.
     */
    private final Map<Long, Transaction> transactionCache = new HashMap<Long, Transaction>();

    public SICompactionState(SDataLib dataLib, DataStore dataStore, TransactionStore transactionStore) {
        this.dataLib = dataLib;
        this.dataStore = dataStore;
        this.transactionStore = transactionStore;
        this.dataToReturn  = new TreeSet<Cell>(new Comparator<Cell>(){
    		@Override
    		public int compare(Cell first, Cell second) {
    			return KeyValue.COMPARATOR.compare(first, second);
    		}
    	});
        this.evaluatedTransactions = new SpliceReusableHashmap<ByteBuffer,Cell>();
    }

    /**
     * Given a list of key-values, populate the results list with possibly mutated values.
     *
	 * @param rawList - the input of key values to process
	 * @param results - the output key values
	 */
    public void mutate(List<Cell> rawList, List<Cell> results) throws IOException {
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
    private void mutate(Cell keyValue) throws IOException {
        final CellType keyValueType = dataStore.getKeyValueType(keyValue);
    	ByteBuffer buffer = ByteBuffer.wrap(CellUtils.getBuffer(keyValue),CellUtils.getTimestampOffset(keyValue),KeyValue.TIMESTAMP_SIZE);
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
    private void mutateCommitTimestamp(ByteBuffer buffer, Cell keyValue) throws IOException {
    		if (evaluatedTransactions.contains(buffer)) {
				if (isFailedCommitTimestamp(evaluatedTransactions.get(buffer))) {
					return;// No Op failed a commit
				} else {
					dataToReturn.add(keyValue);
					return;
				}	
			}
			Transaction transaction = getFromCache(keyValue.getTimestamp());
			final TransactionStatus effectiveStatus = transaction.getEffectiveStatus();
			if (effectiveStatus.isFinished()) {
				if (effectiveStatus.isCommitted()) {
					final Long globalCommitTimestamp = transaction.getEffectiveCommitTimestamp();
					dataToReturn.add(newTransactionTimeStampKeyValue(keyValue,dataLib.encode(globalCommitTimestamp)));
					dataToReturn.add(keyValue);   
				} else {
   				// Finished and not-committed ??
				}
			} else {
				dataToReturn.add(keyValue);
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

	public static Cell newTransactionTimeStampKeyValue(Cell keyValue, byte[] value) {
		return new KeyValue(CellUtils.getBuffer(keyValue),keyValue.getRowOffset(),keyValue.getRowLength(),SIConstants.DEFAULT_FAMILY_BYTES,0,1,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,keyValue.getTimestamp(), KeyValue.Type.Put,value,0,value==null ? 0 : value.length);
	}
	
	public void close() {
		evaluatedTransactions.close();
	}
 
    public static boolean isFailedCommitTimestamp(Cell keyValue) {
    	return keyValue.getValueLength() == 1 && CellUtils.getBuffer(keyValue)[keyValue.getValueOffset()] == SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }
    
	public static int compareKeyValuesByTimestamp(Cell first, Cell second) {
		return Bytes.compareTo(CellUtils.getBuffer(first), CellUtils.getTimestampOffset(first), KeyValue.TIMESTAMP_SIZE,
				CellUtils.getBuffer(second), CellUtils.getTimestampOffset(second), KeyValue.TIMESTAMP_SIZE);
	}
	public static int compareKeyValuesByColumnAndTimestamp(Cell first, Cell second) {
		int compare = Bytes.compareTo(CellUtils.getBuffer(first), first.getQualifierOffset(), first.getQualifierLength(),
                                      CellUtils.getBuffer(second), second.getQualifierOffset(), second.getQualifierLength());
		if (compare != 0)
			return compare;
		return compareKeyValuesByTimestamp(first,second);
	}
    
}
