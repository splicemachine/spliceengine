package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.Provider;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class RegionRollForwardAction<Table,Put> implements RollForwardAction {
        private static Logger LOG = Logger.getLogger(RegionRollForwardAction.class);
		private final Table region;
		private final Provider<TransactionStore> transactionStoreProvider;
		private final Provider<DataStore> dataStoreProvider;
	    private final long maxHeapSize;
	    private final AtomicLong currentHeapSize = new AtomicLong(0l);
	    private final int maxEntries;
	    private final AtomicInteger currentSize = new AtomicInteger(0);
	    private final long timeout;


		public RegionRollForwardAction(Table region,
									long maxHeapSize,
									int maxEntries,
									long timeoutMs,
										Provider<TransactionStore> transactionStoreProvider,
										Provider<DataStore> dataStoreProvider) {
				this.maxHeapSize = maxHeapSize;
				this.maxEntries = maxEntries;
				this.timeout = timeoutMs;
				this.region = region;
				this.transactionStoreProvider = transactionStoreProvider;
				this.dataStoreProvider = dataStoreProvider;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean rollForward(long transactionId, Long effectiveCommitTimestamp, byte[] rowKey) throws IOException {
			try {
				if (effectiveCommitTimestamp != null) {
					// Needs to be buffered
					dataStoreProvider.get().setCommitTimestamp(region, rowKey, transactionId, effectiveCommitTimestamp);	
				} else { 				
			        int entries = currentSize.incrementAndGet();
			        long heap = currentHeapSize.addAndGet(rowKey.length);
			        if(entries>maxEntries||heap>maxHeapSize){
			        	pausedRollForward(entries,heap);
			        }

					final Transaction transaction = transactionStoreProvider.get().getTransaction(transactionId);
	                TransactionStatus status = transaction.getEffectiveStatus();
					final Boolean isFinished = status.isFinished();
					boolean isCommitted = status.isCommitted();
					if (isCommitted) {
						dataStoreProvider.get().setCommitTimestamp(region, rowKey, transaction.getLongTransactionId(), transaction.getEffectiveCommitTimestamp());
					} else if (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ROLLED_BACK)) {
						dataStoreProvider.get().setCommitTimestampToFail(region, rowKey, transaction.getLongTransactionId());
					} else {
	                    LOG.warn("Transaction is finished but it's neither committed nor failed: " + transaction);
	                }
					Tracer.traceRowRollForward(rowKey);
				}
			} catch (NotServingRegionException e) {
				// If the region split and the row is not here, then just skip it
			}
			Tracer.traceTransactionRollForward(transactionId);
			return true;
		}
		
		@Override
		public boolean begin() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public boolean flush() {
			// TODO Auto-generated method stub
			return false;
		}
}
