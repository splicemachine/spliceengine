package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.annotations.ThreadSafe;

/**
 * Factory class for directly acccessing DataStore entities
 * @author Scott Fines
 * Date: 7/3/14
 */
public class TxnDataStore {
		private static final Object lock = new Integer("1");

		private static volatile @ThreadSafe DataStore dataStore;
		private static volatile @ThreadSafe SDataLib dataLib;

		public static DataStore getDataStore() {
				DataStore ds = dataStore;
				if(ds!=null) return ds;

				initialize();
				return dataStore;
		}


		public static SDataLib getDataLib() {
				SDataLib dl = dataLib;
				if(dl!=null) return dl;

				initialize();
				return dataLib;
		}

		private static void initialize() {
				synchronized (lock){
						DataStore ds = dataStore;
						SDataLib dl = dataLib;
						if(ds!=null && dl!=null) return;
						dataLib = SIFactoryDriver.siFactory.getDataLib();
						dataStore = SIFactoryDriver.siFactory.getDataStore();
				}
		}
}
