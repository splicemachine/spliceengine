package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.light.LClientTransactor;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.jmx.ManagedTransactor;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * A Setup class for use in testing code.
 */
@SuppressWarnings("unchecked")
public class TestTransactionSetup {
		final TransactionSchema transactionSchema = new TransactionSchema(
						SIConstants.TRANSACTION_TABLE,
						SIConstants.DEFAULT_FAMILY,
						SIConstants.SI_PERMISSION_FAMILY,
						SIConstants.EMPTY_BYTE_ARRAY,
						SIConstants.TRANSACTION_ID_COLUMN,
						SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN,
						SIConstants.TRANSACTION_PARENT_COLUMN_BYTES,
						SIConstants.TRANSACTION_DEPENDENT_COLUMN_BYTES,
						SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
						SIConstants.TRANSACTION_ADDITIVE_COLUMN_BYTES,
						SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES,
						SIConstants.TRANSACTION_READ_COMMITTED_COLUMN_BYTES,
						SIConstants.TRANSACTION_KEEP_ALIVE_COLUMN,
						SIConstants.TRANSACTION_STATUS_COLUMN,
						SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN,
						SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN,
						SIConstants.TRANSACTION_COUNTER_COLUMN,
						SIConstants.WRITE_TABLE_COLUMN
		);
		byte[] family;
    byte[]  ageQualifier;
    byte[] jobQualifier;
    int  agePosition = 0;
    int jobPosition = 1;
    
    byte[] commitTimestampQualifier;
    byte[] tombstoneQualifier;

    ClientTransactor clientTransactor;
    public Transactor transactor;
		public final TransactionManager control;
    public ManagedTransactor hTransactor;
    public final TransactionStore transactionStore;
    public RollForwardQueue rollForwardQueue;
    public DataStore dataStore;
    public TimestampSource timestampSource = new SimpleTimestampSource();
		public TransactionReadController readController;

    public TestTransactionSetup(StoreSetup storeSetup, boolean simple) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        final STableWriter writer = storeSetup.getWriter();

        final byte[] userColumnsFamilyName = Bytes.toBytes(SIConstants.DEFAULT_FAMILY);
        family = dataLib.encode(userColumnsFamilyName);
        ageQualifier = dataLib.encode(Bytes.toBytes("age"));
        jobQualifier = dataLib.encode(Bytes.toBytes("job"));

        final Map<Long, ImmutableTransaction> immutableCache = CacheMap.makeCache(true);
        final Map<Long, ActiveTransactionCacheEntry> activeCache = CacheMap.makeCache(true);
        final Map<Long, Transaction> cache = CacheMap.makeCache(true);
        final Map<Long, Transaction> committedCache = CacheMap.makeCache(true);
        final Map<Long, Transaction> failedCache = CacheMap.makeCache(true);
        final Map<PermissionArgs, Byte> permissionCache = CacheMap.makeCache(true);
        final ManagedTransactor listener = new ManagedTransactor();
        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, immutableCache, activeCache,
                cache, committedCache, failedCache, permissionCache, SIConstants.committingPause, listener);

        final String tombstoneQualifierString = SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING;
        tombstoneQualifier = SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES;
//        final String commitTimestampQualifierString = SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING;
        commitTimestampQualifier = SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES;
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
								userColumnsFamilyName);
        timestampSource = new SimpleTimestampSource();
				SITransactor.Builder builder = new SITransactor.Builder();
				control = new SITransactionManager(transactionStore,timestampSource,listener);

				readController = new SITransactionReadController(dataStore,dataLib,transactionStore,control);
				//noinspection unchecked
				LClientTransactor cTransactor = new LClientTransactor(dataStore, control, dataLib);
				builder = builder
								.dataLib(dataLib)
								.dataWriter(writer)
								.dataStore(dataStore)
								.transactionStore(transactionStore)
								.clock(storeSetup.getClock())
								.transactionTimeout(SIConstants.transactionTimeout)
								.control(control);
				transactor = builder.build();
        if (!simple) {
            listener.setTransactor(transactor);
            hTransactor = listener;
        }
        clientTransactor = cTransactor;
    }

}
