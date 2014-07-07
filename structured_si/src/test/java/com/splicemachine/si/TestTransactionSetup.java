package com.splicemachine.si;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.light.LClientTransactor;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LTuple;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.readresolve.NoOpReadResolver;
import com.splicemachine.si.impl.store.CompletedTxnCacheSupplier;
import com.splicemachine.si.jmx.ManagedTransactor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * A Setup class for use in testing code.
 */
@SuppressWarnings("unchecked")
public class TestTransactionSetup {
//		final TransactionSchema transactionSchema = new TransactionSchema(
//						SIConstants.TRANSACTION_TABLE,
//						SIConstants.DEFAULT_FAMILY,
//						SIConstants.SI_PERMISSION_FAMILY,
//						SIConstants.EMPTY_BYTE_ARRAY,
//						SIConstants.TRANSACTION_ID_COLUMN,
//						SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN,
//						SIConstants.TRANSACTION_PARENT_COLUMN_BYTES,
//						SIConstants.TRANSACTION_DEPENDENT_COLUMN_BYTES,
//						SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
//						SIConstants.TRANSACTION_ADDITIVE_COLUMN_BYTES,
//						SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES,
//						SIConstants.TRANSACTION_READ_COMMITTED_COLUMN_BYTES,
//						SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN,
//						SIConstants.TRANSACTION_STATUS_COLUMN,
//						SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN,
//						SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN,
//						SIConstants.TRANSACTION_COUNTER_COLUMN,
//						SIConstants.WRITE_TABLE_COLUMN
//		);
		byte[] family;
    byte[]  ageQualifier;
    byte[] jobQualifier;
    int  agePosition = 0;
    int jobPosition = 1;
    
    byte[] commitTimestampQualifier;
    byte[] tombstoneQualifier;

		TxnOperationFactory txnOperationFactory;
    public Transactor transactor;
//		public final TransactionManager control;
    public ManagedTransactor hTransactor;
//    public final TransactionStore transactionStore;
    public RollForward rollForwardQueue;
    public DataStore dataStore;
    public TimestampSource timestampSource = new SimpleTimestampSource();
		public TransactionReadController readController;
		public long transactionTimeout = 1000; //1 second for testing

		public ManualKeepAliveScheduler keepAliveScheduler;
		public final TxnStore txnStore;
		public final TxnSupplier txnSupplier;
		public  TxnLifecycleManager txnLifecycleManager;
		public ReadResolver readResolver = NoOpReadResolver.INSTANCE; //test read-resolvers through different mechanisms

		private final boolean isInMemory;

		public TestTransactionSetup(StoreSetup storeSetup, boolean simple) {
				isInMemory = !(storeSetup instanceof HStoreSetup);
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        final STableWriter writer = storeSetup.getWriter();

        final byte[] userColumnsFamilyName = Bytes.toBytes(SIConstants.DEFAULT_FAMILY);
        family = dataLib.encode(userColumnsFamilyName);
        ageQualifier = dataLib.encode(Bytes.toBytes("age"));
        jobQualifier = dataLib.encode(Bytes.toBytes("job"));

        final ManagedTransactor listener = new ManagedTransactor();
        tombstoneQualifier = SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES;
        commitTimestampQualifier = SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES;

				timestampSource = storeSetup.getTimestampSource();
				ClientTxnLifecycleManager lfManager = new ClientTxnLifecycleManager(timestampSource);

				txnStore = storeSetup.getTxnStore();
        if(isInMemory){
            ((InMemoryTxnStore)txnStore).setLifecycleManager(lfManager);
        }
				txnSupplier = new CompletedTxnCacheSupplier(txnStore,100,16);
				lfManager.setStore(txnStore);
				txnLifecycleManager = lfManager;

        txnOperationFactory = new SimpleOperationFactory(txnSupplier);

				//noinspection unchecked
				dataStore = new DataStore(dataLib, reader, writer,
								SIConstants.SI_NEEDED,
								SIConstants.SI_NEEDED_VALUE_BYTES,
								SIConstants.SI_TRANSACTION_ID_KEY,
								SIConstants.SI_DELETE_PUT,
                commitTimestampQualifier,
								tombstoneQualifier,
								SIConstants.EMPTY_BYTE_ARRAY,
								SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
								SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,
								userColumnsFamilyName,
								txnSupplier,txnLifecycleManager
								);
				SITransactor.Builder builder = new SITransactor.Builder();
//				control = new SITransactionManager(transactionStore,timestampSource,listener);


				keepAliveScheduler = new ManualKeepAliveScheduler(txnStore);
        ((ClientTxnLifecycleManager)txnLifecycleManager).setKeepAliveScheduler(keepAliveScheduler);

				readController = new SITransactionReadController(dataStore,dataLib, txnStore,txnLifecycleManager);
				//noinspection unchecked
//				LClientTransactor cTransactor = new LClientTransactor(dataStore, txnLifecycleManager, dataLib);
				builder = builder
								.dataLib(dataLib)
								.dataWriter(writer)
								.dataStore(dataStore)
								.txnStore(txnSupplier) //use the cache for completed transactions
								.clock(storeSetup.getClock())
                .operationFactory(txnOperationFactory)
								.transactionTimeout(SIConstants.transactionTimeout);
//								.control(control);
				transactor = builder.build();

        if (!simple) {
            listener.setTransactor(transactor);
            hTransactor = listener;
        }
//        HTransactorFactory.setTransactor(hTransactor);
    }

		/*
		 * The following methods are in place to bridge the goofiness gap between real code (i.e. HBase) and
		 * the stupid test code, without requiring odd production-level classes and methods which don't have good
		 * type signatures and don't make sense within the system. Someday, we'll remove the test Operation logic
		 * entirely and replace it with an in-memory HBase installation
		 */

		public OperationWithAttributes convertTestTypePut(Put put) {
				if(isInMemory){
						OperationWithAttributes owa= new LTuple(put.getRow(), Lists.newArrayList(Iterables.concat(put.getFamilyMap().values())));
						copyAttributes(put,owa);
						return owa;
				}else return put;
		}

		protected void copyAttributes(OperationWithAttributes source,OperationWithAttributes dest) {
				Map<String,byte[]> attributesMap = source.getAttributesMap();
				for(Map.Entry<String,byte[]> attribute:attributesMap.entrySet()){
						dest.setAttribute(attribute.getKey(),attribute.getValue());
				}
		}

		public OperationWithAttributes convertTestTypeScan(Scan scan,Long effectiveTimestamp){
				if(isInMemory){
						List<List<byte[]>> columns = Lists.newArrayList();
						List<byte[]> families = Lists.newArrayList();
						Map<byte[],NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
						for(byte[] family:families){
								List<byte[]> columnsForFamily = Lists.newArrayList(familyMap.get(family));
								columns.add(columnsForFamily);
						}
            if(families.size()<=0)
                families = null;
            if(columns.size()<=0)
                columns = null;

						OperationWithAttributes owa = new LGet(scan.getStartRow(),scan.getStopRow(),
										families,
										columns,effectiveTimestamp,scan.getMaxVersions());
						copyAttributes(scan,owa);
						return owa;
				}else return scan;
		}

		public OperationWithAttributes convertTestTypeGet(Get scan,Long effectiveTimestamp){
				if(isInMemory){
						List<List<byte[]>> columns = Lists.newArrayList();
						List<byte[]> families = Lists.newArrayList();
						Map<byte[],NavigableSet<byte[]>> familyMap = scan.getFamilyMap();
						for(byte[] family:familyMap.keySet()){
								families.add(family);
								List<byte[]> columnsForFamily = Lists.newArrayList(familyMap.get(family));
								columns.add(columnsForFamily);
						}
            if(families.size()<=0)
                families = null;
            if(columns.size()<=0)
                columns = null;

						OperationWithAttributes owa = new LGet(scan.getRow(),scan.getRow(),
										families,
										columns,effectiveTimestamp,scan.getMaxVersions());
						copyAttributes(scan,owa);
						return owa;
				}else return scan;

		}
}
