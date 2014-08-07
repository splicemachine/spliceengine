package com.splicemachine.si.impl.rollforward;

import com.google.common.base.Supplier;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.RollForwardAction;
import com.splicemachine.si.impl.Tracer;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class PushForwardAction<Table,Put extends OperationWithAttributes> implements RollForwardAction {
		protected static Logger LOG = Logger.getLogger(PushForwardAction.class);
        protected Table region;
		protected Supplier<TransactionStore> transactionStoreProvider;
		protected Supplier<DataStore> dataStoreProvider;
	    protected static AtomicLong pushedForwardSize = new AtomicLong(0);	  

	    
		public PushForwardAction(Table region,
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
				Pair<OperationWithAttributes,Long>[] regionMutations = new Pair[size];
				pushedForwardSize.addAndGet(size);
				for (int i =0; i<size; i++) {
					RollForwardEvent rollForwardEvent = rollForwardEvents.get(i);
					regionMutations[i] = Pair.newPair(dataStoreProvider.get().generateCommitTimestamp(region, rollForwardEvent.getRowKey(), rollForwardEvent.getTransactionId(), rollForwardEvent.getEffectiveTimestamp()),(Long) null);
				}
				dataStoreProvider.get().writeBatch(region, regionMutations);
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
			return String.format("PushForwardAction={region=%s}",region);
		}

}
