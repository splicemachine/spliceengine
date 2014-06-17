package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.impl.*;
import com.splicemachine.utils.Provider;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class PushForwardAction<Table,Put extends OperationWithAttributes> implements RollForwardAction {
		protected static Logger LOG = Logger.getLogger(PushForwardAction.class);
        protected Table region;
		protected Provider<TransactionStore> transactionStoreProvider;
		protected Provider<DataStore> dataStoreProvider;
	    
		public PushForwardAction(Table region,
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
				Pair<OperationWithAttributes,Long>[] regionMutations = new Pair[size];		
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
}
