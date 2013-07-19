package com.splicemachine.si;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.ActiveTransactionCacheEntry;
import com.splicemachine.si.impl.CacheMap;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.ImmutableTransaction;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionSchema;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.si.jmx.ManagedTransactor;

import java.util.Map;

public class TransactorSetup extends SIConstants {
    final TransactionSchema transactionSchema = new TransactionSchema(SpliceConstants.TRANSACTION_TABLE, "siFamily",
            -1, "id", "begin", "parent", "dependent", "allowWrites", "readUncommited", "readCommitted",
            "keepAlive", "status", "commit", "globalCommit", "counter");
    Object family;
    Object ageQualifier;
    Object jobQualifier;
    Object commitTimestampQualifier;
    Object tombstoneQualifier;

    ClientTransactor clientTransactor;
    public Transactor transactor;
    public ManagedTransactor hTransactor;
    public final TransactionStore transactionStore;
    public RollForwardQueue rollForwardQueue;
    public DataStore dataStore;

    public TransactorSetup(StoreSetup storeSetup, boolean simple) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        final STableWriter writer = storeSetup.getWriter();

        final String userColumnsFamilyName = DEFAULT_FAMILY;
        family = dataLib.encode(userColumnsFamilyName);
        ageQualifier = dataLib.encode("age");
        jobQualifier = dataLib.encode("job");

        final Map<Long, ImmutableTransaction> immutableCache = CacheMap.makeCache(true);
        final Map<Long, ActiveTransactionCacheEntry> activeCache = CacheMap.makeCache(true);
        final Map<Long, Transaction> cache = CacheMap.makeCache(true);
        final Map<Long, Transaction> committedCache = CacheMap.makeCache(true);
        final Map<Long, Transaction> failedCache = CacheMap.makeCache(true);
        final ManagedTransactor listener = new ManagedTransactor();
        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, immutableCache, activeCache,
                cache, committedCache, failedCache, 1000, listener);

        final String tombstoneQualifierString = SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING;
        tombstoneQualifier = dataLib.encode(tombstoneQualifierString);
        final String commitTimestampQualifierString = SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING;
        commitTimestampQualifier = dataLib.encode(commitTimestampQualifierString);
        dataStore = new DataStore(dataLib, reader, writer, "si_needed", SI_NEEDED_VALUE, ONLY_SI_FAMILY_NEEDED_VALUE,
                "si_include_uncommitted_as_of_start", 1, "si_transaction_id", "si_delete_put", SNAPSHOT_ISOLATION_FAMILY,
                commitTimestampQualifierString, tombstoneQualifierString, -1, "zombie", -2, userColumnsFamilyName);
        transactor = new SITransactor(new SimpleTimestampSource(), dataLib, writer,
                dataStore,
                transactionStore, storeSetup.getClock(), 1500, new NoOpHasher(), listener);
        if (!simple) {
            listener.setTransactor(transactor);
            hTransactor = listener;
        }
        clientTransactor = transactor;
    }

}
