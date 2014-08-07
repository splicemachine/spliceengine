package com.splicemachine.si.api;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.table.BetterHTablePool;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HPoolTableSource;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.annotations.ThreadSafe;
import org.apache.hadoop.hbase.HConstants;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

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

						dl = new HDataLib();

						BetterHTablePool hTablePool = new BetterHTablePool(new SpliceHTableFactory(),
										SpliceConstants.tablePoolCleanerInterval, TimeUnit.SECONDS,
										SpliceConstants.tablePoolMaxSize,SpliceConstants.tablePoolCoreSize);
						final HPoolTableSource tableSource = new HPoolTableSource(hTablePool);
            final STableReader reader;
            try {
                reader = new HTableReader(tableSource);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final STableWriter writer = new HTableWriter();
						//noinspection unchecked
						dataStore = new DataStore(dl,reader,writer,
										SIConstants.SI_NEEDED,
										SIConstants.SI_NEEDED_VALUE_BYTES,
										SIConstants.SI_TRANSACTION_ID_KEY,
										SIConstants.SI_DELETE_PUT,
										SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
										SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
										HConstants.EMPTY_BYTE_ARRAY,
										SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
										SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,
										SIConstants.DEFAULT_FAMILY_BYTES,
										TransactionStorage.getTxnSupplier(),
										TransactionLifecycle.getLifecycleManager()
										);

						dataLib = dl;
				}
		}
}
