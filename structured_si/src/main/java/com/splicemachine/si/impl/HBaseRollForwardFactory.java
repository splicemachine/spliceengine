package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardFactory;
import com.splicemachine.si.coprocessors.RegionRollForwardAction;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.utils.Provider;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public class HBaseRollForwardFactory implements RollForwardFactory<byte[],HbRegion>{
		private final Provider<TransactionStore> transactionStore;
		private final Provider<DataStore> dataStore;

		public HBaseRollForwardFactory(Provider<TransactionStore> transactionStore, Provider<DataStore> dataStore) {
				this.transactionStore = transactionStore;
				this.dataStore = dataStore;
		}

		@Override
		public RollForwardAction newAction(HbRegion table) {
				return new RegionRollForwardAction(table,transactionStore,dataStore);
		}
}
