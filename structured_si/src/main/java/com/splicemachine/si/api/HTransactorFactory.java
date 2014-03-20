package com.splicemachine.si.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

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
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.ActiveTransactionCacheEntry;
import com.splicemachine.si.impl.CacheMap;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.HBaseClientTransactor;
import com.splicemachine.si.impl.HBaseRollForwardFactory;
import com.splicemachine.si.impl.ImmutableTransaction;
import com.splicemachine.si.impl.PermissionArgs;
import com.splicemachine.si.impl.SITransactionManager;
import com.splicemachine.si.impl.SITransactionReadController;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.si.impl.SystemClock;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionSchema;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.si.jmx.TransactorStatus;
import com.splicemachine.si.txn.ZooKeeperStatTimestampSource;
import com.splicemachine.utils.Provider;
import com.splicemachine.utils.Providers;
import com.splicemachine.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;

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
		private static volatile SITransactionReadController<Get,Scan,Delete,Mutation,Put> readController;
		private static volatile RollForwardFactory<byte[], HbRegion> rollForwardFactory;
		private static volatile TransactionStore transactionStore;
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

		public static RollForwardFactory<byte[],HbRegion> getRollForwardFactory(){
				if(rollForwardFactory!=null)
						return rollForwardFactory;

				synchronized (HTransactorFactory.class){
						rollForwardFactory = new HBaseRollForwardFactory(new Provider<TransactionStore>() {
								@Override
								public TransactionStore get() {
										initializeIfNeeded();
										return transactionStore;
								}
						},new Provider<DataStore>() {
								@Override
								public DataStore get() {
										initializeIfNeeded();
										return dataStore;
								}
						});
				}

				return rollForwardFactory;
		}

		public static SITransactionReadController<Get,Scan,Delete,Mutation,Put> getTransactionReadController(){
				initializeIfNeeded();
				return readController;
		}

		public static TransactionStore getTransactionStore() {
				initializeIfNeeded();
				return transactionStore;
		}

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
						try {
								ZkUtils.refreshZookeeper();
						} catch (InterruptedException e) {
								throw new RuntimeException(e);
						} catch (KeeperException e) {
								throw new RuntimeException(e);
						}
						TimestampSource timestampSource = new ZooKeeperStatTimestampSource(ZkUtils.getRecoverableZooKeeper(),zkSpliceTransactionPath);
						BetterHTablePool hTablePool = new BetterHTablePool(new SpliceHTableFactory(true,1),
										SpliceConstants.tablePoolCleanerInterval, TimeUnit.SECONDS,
										SpliceConstants.tablePoolMaxSize,SpliceConstants.tablePoolCoreSize);
						final HPoolTableSource tableSource = new HPoolTableSource(hTablePool);
						SDataLib dataLib = new HDataLib();
						final STableReader reader = new HTableReader(tableSource);
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
										TRANSACTION_KEEP_ALIVE_COLUMN, TRANSACTION_STATUS_COLUMN,
										TRANSACTION_COMMIT_TIMESTAMP_COLUMN,
										TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN,
										TRANSACTION_COUNTER_COLUMN,
										WRITE_TABLE_COLUMN
						);
						ManagedTransactor builderTransactor = new ManagedTransactor();
						transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer,
										immutableCache, activeCache, cache, committedCache, failedCache, permissionCache, SIConstants.committingPause, builderTransactor);

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
										DEFAULT_FAMILY_BYTES); //user columnFamily
						if(transactionManager ==null)
								transactionManager = new SITransactionManager(transactionStore,timestampSource,builderTransactor);
						if(clientTransactor==null)
								clientTransactor = new HBaseClientTransactor(dataStore,transactionManager,dataLib);
						if(rollForwardFactory ==null)
								rollForwardFactory = new HBaseRollForwardFactory(Providers.basicProvider(transactionStore),
												Providers.basicProvider(dataStore));

						if(readController==null)
								readController = new SITransactionReadController<Get,Scan,Delete,Mutation,Put>
												(dataStore,dataLib,transactionStore,transactionManager);
						Transactor transactor = new SITransactor.Builder()
										.dataLib(dataLib)
										.dataWriter(writer)
										.dataStore(dataStore)
										.transactionStore(transactionStore)
										.clock(new SystemClock())
										.transactionTimeout(transactionTimeout)
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

}
