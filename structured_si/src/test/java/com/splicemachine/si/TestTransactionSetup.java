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
    final TransactionSchema transactionSchema = new TransactionSchema(SpliceConstants.TRANSACTION_TABLE, Bytes.toBytes("siFamily"),
            Bytes.toBytes("permissionFamily"), -1, Bytes.toBytes("id"), Bytes.toBytes("begin"), Bytes.toBytes("parent"), Bytes.toBytes("dependent"),
						Bytes.toBytes("allowWrites"), Bytes.toBytes("additive"), Bytes.toBytes("readUncommited"),
            Bytes.toBytes("readCommitted"), Bytes.toBytes("keepAlive"), Bytes.toBytes("status"), Bytes.toBytes("commit"), Bytes.toBytes("globalCommit"), Bytes.toBytes("counter"));
		byte[] family;
    byte[]  ageQualifier;
    byte[] jobQualifier;
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
				dataStore = new DataStore(dataLib, reader, writer, "si_needed",
								SIConstants.SI_NEEDED_VALUE,
								SIConstants.ONLY_SI_FAMILY_NEEDED_VALUE,
                "si_transaction_id", "si_delete_put",
								SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,
                commitTimestampQualifier, tombstoneQualifier, -1, "zombie", -2, userColumnsFamilyName);
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
								.control(control)
								.clientTransactor(cTransactor);
				transactor = builder.build();
        if (!simple) {
            listener.setTransactor(transactor);
            hTransactor = listener;
        }
        clientTransactor = cTransactor;
    }

}
