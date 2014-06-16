package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.Provider;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class RegionRollForwardAction<Table> implements RollForwardAction {
        private static Logger LOG = Logger.getLogger(RegionRollForwardAction.class);
		private final Table region;
		private final Provider<TransactionStore> transactionStoreProvider;
		private final Provider<DataStore> dataStoreProvider;

		public RegionRollForwardAction(Table region,
										Provider<TransactionStore> transactionStoreProvider,
										Provider<DataStore> dataStoreProvider) {
				this.region = region;
				this.transactionStoreProvider = transactionStoreProvider;
				this.dataStoreProvider = dataStoreProvider;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Boolean rollForward(long transactionId, Long effectiveCommitTimestamp, byte[] rowKey) throws IOException {
			try {
				if (effectiveCommitTimestamp != null) {
					dataStoreProvider.get().setCommitTimestamp(region, rowKey, transactionId, effectiveCommitTimestamp);	
				} else { 				
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
}
