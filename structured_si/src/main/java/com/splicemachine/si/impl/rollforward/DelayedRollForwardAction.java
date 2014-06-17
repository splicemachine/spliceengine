package com.splicemachine.si.impl.rollforward;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.Provider;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class DelayedRollForwardAction<Table,Put extends OperationWithAttributes> implements RollForwardAction {
        private static Logger LOG = Logger.getLogger(DelayedRollForwardAction.class);
        protected Table region;
		protected Provider<TransactionStore> transactionStoreProvider;
		protected Provider<DataStore> dataStoreProvider;
	    protected int mutationBucket;
	    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5,
	            new ThreadFactoryBuilder().setNameFormat("delayedRollForwardEvent-%d").build());

	    
		public DelayedRollForwardAction(Table region,
										Provider<TransactionStore> transactionStoreProvider,
										Provider<DataStore> dataStoreProvider) {
				this.region = region;
				this.transactionStoreProvider = transactionStoreProvider;
				this.dataStoreProvider = dataStoreProvider;
		}

		@Override
		public boolean write(List<RollForwardEvent> rollForwardEvents) {
			try {
				int size = rollForwardEvents.size();
				List<Pair<OperationWithAttributes,Long>> regionMutations = new ArrayList<Pair<OperationWithAttributes,Long>>();		
				for (int i =0; i<size; i++) {
					RollForwardEvent rollForwardEvent = rollForwardEvents.get(i);
					final Transaction transaction = transactionStoreProvider.get().getTransaction(rollForwardEvent.getTransactionId());
					TransactionStatus status = transaction.getEffectiveStatus();
					if (!status.isFinished())
						continue;
					if (status.isCommitted()) {
						regionMutations.add(Pair.newPair(dataStoreProvider.get().generateCommitTimestamp(region, rollForwardEvent.getRowKey(), transaction.getLongTransactionId(), transaction.getEffectiveCommitTimestamp()), (Long) null));
					} else if (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ROLLED_BACK)) {
						regionMutations.add(Pair.newPair(dataStoreProvider.get().generateCommitTimestampToFail(region, rollForwardEvent.getRowKey(), transaction.getLongTransactionId()), (Long) null));
					} else {
		                LOG.warn("Transaction is finished but it's neither committed nor failed: " + transaction);
		            }
				}
				dataStoreProvider.get().writeBatch(region, regionMutations.toArray(new Pair[regionMutations.size()]));
				
				if (Tracer.isTracingRowRollForward()) { // Notify
					for (int i =0; i<size; i++) {
						RollForwardEvent rollForwardEvent = rollForwardEvents.get(i);
						Tracer.traceRowRollForward(rollForwardEvent.getRowKey());
					}	
				}

				
				return true;
			} catch (IOException e) {
				SpliceLogUtils.warn(LOG, "Could not push forward data");
				return false;
			}								
		}
		
}
