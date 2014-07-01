package com.splicemachine.si.api;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.table.BetterHTablePool;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.*;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.txnclient.CoprocessorTxnStore;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.si.jmx.TransactorStatus;
import com.splicemachine.si.txn.SpliceTimestampSource;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Used to construct a transactor object that is bound to the HBase types and that provides SI functionality. This is
 * the main entry point for applications using SI. Keeps track of a global, static instance that is shared by all callers.
 * Also defines the various configuration options and plugins for the transactor instance.
 */
public class HTransactorFactory extends SIConstants {
    private final static Map<Long, ImmutableTransaction> immutableCache = CacheMap.makeCache(true);
    private final static Map<Long, ActiveTransactionCacheEntry> activeCache = CacheMap.makeCache(true);
    private final static Map<Long, Transaction> cache = CacheMap.makeCache(true);
    private final static Map<Long, Transaction> committedCache = CacheMap.makeCache(true);
    private final static Map<Long, Transaction> failedCache = CacheMap.makeCache(true);
    private final static Map<PermissionArgs, Byte> permissionCache = CacheMap.makeCache(true);

		private static volatile boolean initialized;
    private static volatile ManagedTransactor managedTransactor;
		private static volatile TransactionManager transactionManager;
		private static volatile ClientTransactor clientTransactor;
		private static volatile SITransactionReadController< Get, Scan, Delete, Put> readController;
		private static volatile RollForwardQueueFactory<byte[], HbRegion> rollForwardQueueFactory;
//		private static volatile TransactionStore transactionStore;
		private static volatile DataStore dataStore;

		public static void setTransactor(ManagedTransactor managedTransactorToUse) {
        managedTransactor = managedTransactorToUse;
    }

    public static TransactorStatus getTransactorStatus() {
				initializeIfNeeded();
				return managedTransactor;
    }

    public static TransactionManager getTransactionManager() {
				initializeIfNeeded();
				return transactionManager;
    }

    @SuppressWarnings("unchecked")
		public static ClientTransactor<Put, Get, Scan, Mutation> getClientTransactor() {
				initializeIfNeeded();
				return clientTransactor;
    }

    public static Transactor<IHTable, Mutation,Put> getTransactor() {
				initializeIfNeeded();
				return managedTransactor.getTransactor();
    }

		public static RollForwardQueueFactory<byte[],HbRegion> getRollForwardQueueFactory(){
				if(rollForwardQueueFactory !=null)
						return rollForwardQueueFactory;
				throw new UnsupportedOperationException("REPLACE WITH ROLL FORWARD");

//				synchronized (HTransactorFactory.class){
//						rollForwardQueueFactory = new HBaseRollForwardQueueFactory(new Provider<TransactionStore>() {
//								@Override
//								public TransactionStore get() {
//										initializeIfNeeded();
////										return transactionStore;
//								}
//						},new Provider<DataStore>() {
//								@Override
//								public DataStore get() {
//										initializeIfNeeded();
//										return dataStore;
//								}
//						});
//				}

//				return rollForwardQueueFactory;
		}

		public static TransactionReadController<Get,Scan> getTransactionReadController(){
				initializeIfNeeded();
				return readController;
		}

//		public static TransactionStore getTransactionStore() {
//				initializeIfNeeded();
////				return transactionStore;
//		}

		@SuppressWarnings("unchecked")
		private static void initializeIfNeeded(){
				if(initialized) return;

				synchronized (HTransactorFactory.class) {
						if(initialized) return;
						if(managedTransactor!=null && transactionManager !=null && clientTransactor!=null){
								//it was externally created
								initialized = true;
								return;
						}

						// Left here commented out for reference, but as of 0.5.1 we no longer use ZooKeeperStatTimestampSource.
						// TimestampSource timestampSource = new ZooKeeperStatTimestampSource(ZkUtils.getRecoverableZooKeeper(),zkSpliceTransactionPath);
						TimestampSource timestampSource = new SpliceTimestampSource(ZkUtils.getRecoverableZooKeeper());
						BetterHTablePool hTablePool = new BetterHTablePool(new SpliceHTableFactory(),
										SpliceConstants.tablePoolCleanerInterval, TimeUnit.SECONDS,
										SpliceConstants.tablePoolMaxSize,SpliceConstants.tablePoolCoreSize);
						final HPoolTableSource tableSource = new HPoolTableSource(hTablePool);
						SDataLib dataLib = new HDataLib();
            final STableReader reader;
            try {
                reader = new HTableReader(tableSource);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            final STableWriter writer = new HTableWriter();
						final TransactionSchema transactionSchema = new TransactionSchema(TRANSACTION_TABLE,
										DEFAULT_FAMILY,
										SI_PERMISSION_FAMILY,
										EMPTY_BYTE_ARRAY,
										TRANSACTION_ID_COLUMN,
										TRANSACTION_START_TIMESTAMP_COLUMN,
										TRANSACTION_PARENT_COLUMN_BYTES,
										TRANSACTION_DEPENDENT_COLUMN_BYTES,
										TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
										TRANSACTION_ADDITIVE_COLUMN_BYTES,
										TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES,
										TRANSACTION_READ_COMMITTED_COLUMN_BYTES,
										TRANSACTION_KEEP_ALIVE_COLUMN,
                    TRANSACTION_STATUS_COLUMN,
										TRANSACTION_COMMIT_TIMESTAMP_COLUMN,
										TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN,
										TRANSACTION_COUNTER_COLUMN,
										WRITE_TABLE_COLUMN
						);
						ManagedTransactor builderTransactor = new ManagedTransactor();
//						transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer,
//										immutableCache, activeCache, cache, committedCache, failedCache, permissionCache, SIConstants.committingPause, builderTransactor);

						dataStore = new DataStore(dataLib, reader, writer,
										SI_NEEDED, //siNeededAttribute
										SI_NEEDED_VALUE_BYTES ,             //the bytes for the siNeededAttribute
										SI_TRANSACTION_ID_KEY, //transactionIdAttribute
										SI_DELETE_PUT,  //deletePutAttribute
										SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES, //commitTimestampQualifier
										SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES, //tombstoneQualifier
										EMPTY_BYTE_ARRAY,  //siNull
										SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES, //siAntiTombstoneValue
										SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,  //siFail
										DEFAULT_FAMILY_BYTES,
										new LazyTxnSupplier(txnSupplier),
										lifecycleManager ); //user columnFamily
//						if(transactionManager ==null)
//								transactionManager = new SITransactionManager(transactionStore,timestampSource,builderTransactor);
						if(clientTransactor==null){
								//use a lazy transaction wrapper here to avoid excessive network calls when looking up parent transactions
								clientTransactor = new HBaseClientTransactor(dataStore,lifecycleManager,dataLib);
						}
//						if(rollForwardQueueFactory ==null)
//								rollForwardQueueFactory = new HBaseRollForwardQueueFactory(Providers.basicProvider(transactionStore),
//												Providers.basicProvider(dataStore));

						if(readController==null)
								readController = new SITransactionReadController<
												Get,Scan,Delete,Put>(dataStore,dataLib, txnSupplier,lifecycleManager);
						Transactor transactor = new SITransactor.Builder()
										.dataLib(dataLib)
										.dataWriter(writer)
										.dataStore(dataStore)
//										.transactionStore(transactionStore)
										.clock(new SystemClock())
										.transactionTimeout(transactionTimeout)
										.txnStore(txnSupplier)
										.control(transactionManager).build();
						builderTransactor.setTransactor(transactor);
						if(managedTransactor==null)
								managedTransactor = builderTransactor;

						initialized = true;
				}
		}

		public static void setTransactionManager(TransactionManager transactionManager) {
				HTransactorFactory.transactionManager = transactionManager;
		}
		public static void setClientTransactor(ClientTransactor clientTransactor){
				HTransactorFactory.clientTransactor = clientTransactor;
		}

		public static TimestampSource getTimestampSource() {
				throw new UnsupportedOperationException("IMPLEMENT");
		}
}
