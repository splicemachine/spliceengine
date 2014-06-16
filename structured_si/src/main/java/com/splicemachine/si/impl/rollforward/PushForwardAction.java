package com.splicemachine.si.impl.rollforward;

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
public class PushForwardAction<Table,Put extends OperationWithAttributes> implements RollForwardAction {
		protected static Logger LOG = Logger.getLogger(PushForwardAction.class);
        protected Table region;
		protected Provider<TransactionStore> transactionStoreProvider;
		protected Provider<DataStore> dataStoreProvider;
	    protected HashMap<Table,List<Pair<OperationWithAttributes,Long>>> regionMutations;
	    
		public PushForwardAction(Table region,
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
				if (regionMutations.containsKey(region)) {
					regionMutations.get(region).add(Pair.newPair(dataStoreProvider.get().generateCommitTimestamp(region, rowKey, transactionId, effectiveCommitTimestamp),(Long) null));
				} else {
					List<Pair<OperationWithAttributes,Long>> mutations = new ArrayList<Pair<OperationWithAttributes,Long>>();
					mutations.add(Pair.newPair(dataStoreProvider.get().generateCommitTimestamp(region, rowKey, transactionId, effectiveCommitTimestamp),(Long) null));
					regionMutations.put(region, mutations);					
				}					
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
