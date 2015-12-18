package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.impl.driver.SIDriver;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Factory class for directly acccessing DataStore entities
 * @author Scott Fines
 * Date: 7/3/14
 */
@SuppressFBWarnings({"DL_SYNCHRONIZATION_ON_UNSHARED_BOXED_PRIMITIVE","DM_NUMBER_CTOR"})
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
						dataLib = SIDriver.siFactory.getDataLib();
						dataStore = SIDriver.siFactory.getDataStore();
				}
		}
}
