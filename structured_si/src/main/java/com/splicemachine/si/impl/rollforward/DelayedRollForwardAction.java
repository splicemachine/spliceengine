package com.splicemachine.si.impl.rollforward;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class DelayedRollForwardAction<Table,Put extends OperationWithAttributes> implements RollForwardAction {
        private static Logger LOG = Logger.getLogger(DelayedRollForwardAction.class);
        protected static AtomicLong committedRollForwards = new AtomicLong(0);	  
        protected static AtomicLong siFailRollForwards = new AtomicLong(0);	  
        protected static AtomicLong notFinishedRollForwards = new AtomicLong(0);	  
        
        protected Table region;
		protected Supplier<TransactionStore> transactionStoreProvider;
		protected Supplier<DataStore> dataStoreProvider;
	    protected int mutationBucket;
	    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5,
	            new ThreadFactoryBuilder().setNameFormat("delayedRollForwardEvent-%d").build());

	    
		public DelayedRollForwardAction(Table region,
										Supplier<TransactionStore> transactionStoreProvider,
										Supplier<DataStore> dataStoreProvider) {
				this.region = region;
				this.transactionStoreProvider = transactionStoreProvider;
				this.dataStoreProvider = dataStoreProvider;
		}

		@Override
		public boolean write(List<RollForwardEvent> rollForwardEvents) {
			if (LOG.isDebugEnabled())
				SpliceLogUtils.debug(LOG, "write with %d rollForwardEvents",rollForwardEvents.size());
			try {
				int size = rollForwardEvents.size();
				List<Pair<OperationWithAttributes,Long>> regionMutations = new ArrayList<Pair<OperationWithAttributes,Long>>();		
				for (int i =0; i<size; i++) {
					RollForwardEvent rollForwardEvent = rollForwardEvents.get(i);
					final Transaction transaction = transactionStoreProvider.get().getTransaction(rollForwardEvent.getTransactionId());
					TransactionStatus status = transaction.getEffectiveStatus();
					if (!status.isFinished()) {
						notFinishedRollForwards.incrementAndGet();
						if (LOG.isTraceEnabled())
							SpliceLogUtils.trace(LOG, "transaction not finished missed on rollForwardEvent=%s",rollForwardEvent);
						continue;
					}
					if (status.isCommitted()) {
						committedRollForwards.incrementAndGet();
						if (LOG.isTraceEnabled())
							SpliceLogUtils.trace(LOG, "transaction committed, delayed RollForwardHit on rollForwardEvent=%s",rollForwardEvent);
						regionMutations.add(Pair.newPair(dataStoreProvider.get().generateCommitTimestamp(region, rollForwardEvent.getRowKey(), transaction.getLongTransactionId(), transaction.getEffectiveCommitTimestamp()), (Long) null));
					} else if (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ROLLED_BACK)) {
						siFailRollForwards.incrementAndGet();
						if (LOG.isTraceEnabled())
							SpliceLogUtils.trace(LOG, "transaction errored, delayed RollForwardHit on rollForwardEvent=%s",rollForwardEvent);
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

		@Override
		public String toString() {
			return String.format("DelayedRollForwardAction={region=%s}",region);
		}
		
}
