package com.splicemachine.si;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.impl.ActiveTransactionCacheEntry;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.ImmutableTransaction;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionSchema;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.si.jmx.ManagedTransactor;

import java.util.concurrent.TimeUnit;

public class TransactorSetup extends SIConstants {
    final TransactionSchema transactionSchema = new TransactionSchema(SpliceConstants.TRANSACTION_TABLE, "siFamily",
            "siChildrenFamily", -1, "id", "begin", "parent", "dependent", "allowWrites", "readUncommited", "readCommitted",
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

    public TransactorSetup(StoreSetup storeSetup, boolean simple) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        final STableWriter writer = storeSetup.getWriter();

        final String userColumnsFamilyName = DEFAULT_FAMILY;
        family = dataLib.encode(userColumnsFamilyName);
        ageQualifier = dataLib.encode("age");
        jobQualifier = dataLib.encode("job");

        final Cache<Long, ImmutableTransaction> immutableCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, ActiveTransactionCacheEntry> activeCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, Transaction> cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, Transaction> committedCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, Transaction> failedCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final ManagedTransactor listener = new ManagedTransactor(immutableCache, activeCache, cache);
        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, immutableCache, activeCache,
                cache, committedCache, failedCache, 1000, listener);

        final String tombstoneQualifierString = SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING;
        tombstoneQualifier = dataLib.encode(tombstoneQualifierString);
        final String commitTimestampQualifierString = SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING;
        commitTimestampQualifier = dataLib.encode(commitTimestampQualifierString);
        transactor = new SITransactor(new SimpleTimestampSource(), dataLib, writer,
                new DataStore(dataLib, reader, writer, "si-needed", SI_NEEDED_VALUE, ONLY_SI_FAMILY_NEEDED_VALUE,
                        "si-uncommitted", 1, "si-transaction-id", "si-delete-put", SNAPSHOT_ISOLATION_FAMILY,
                        commitTimestampQualifierString, tombstoneQualifierString,
                        SNAPSHOT_ISOLATION_PLACE_HOLDER_COLUMN_STRING,
                        -1, -2, userColumnsFamilyName),
                transactionStore, storeSetup.getClock(), 1500, listener);
        if (!simple) {
            listener.setTransactor(transactor);
            hTransactor = listener;
        }
        clientTransactor = transactor;
    }

}
