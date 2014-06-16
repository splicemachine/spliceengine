package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.Provider;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class DelayedRollForwardAction<Table,Put extends OperationWithAttributes> implements RollForwardAction {
        private static Logger LOG = Logger.getLogger(DelayedRollForwardAction.class);

        protected Table region;
		protected Provider<TransactionStore> transactionStoreProvider;
		protected Provider<DataStore> dataStoreProvider;
	    protected HashMap<Table,List<Pair<OperationWithAttributes,Long>>> regionMutations;
	    
		public DelayedRollForwardAction(Table region,
										Provider<TransactionStore> transactionStoreProvider,
										Provider<DataStore> dataStoreProvider) {
				this.region = region;
				this.transactionStoreProvider = transactionStoreProvider;
				this.dataStoreProvider = dataStoreProvider;
		}

		@Override
		@SuppressWarnings("unchecked")
		public boolean rollForward(long transactionId, Long effectiveCommitTimestamp, byte[] rowKey) throws IOException {
			try {
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
			} catch (NotServingRegionException e) {
				// If the region split and the row is not here, then just skip it
			}
			Tracer.traceTransactionRollForward(transactionId);
			return true;
		}
		
		@Override
		public boolean begin() {
			regionMutations.clear();
			return true;
		}

		@Override
		public boolean flush() {
			try {
				for (Table table: regionMutations.keySet()) {
					dataStoreProvider.get().writeBatch(region, regionMutations.get(table).toArray(new Pair[regionMutations.get(table).size()]));					
				}				
			} catch (IOException e) { // Swallow Error
				SpliceLogUtils.error(LOG, "flush failed ",e);
				return false;
			}
			return true;
		}
}
