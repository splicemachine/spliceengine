package com.splicemachine.si;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.com.splicemachine.si.api.hbase.HTransactor;
import com.splicemachine.si.data.hbase.HTransactorAdapter;
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

import java.util.concurrent.TimeUnit;

public class TransactorSetup extends SIConstants {
    final TransactionSchema transactionSchema = new TransactionSchema(SpliceConstants.TRANSACTION_TABLE, "siFamily",
            "siChildrenFamily", -1, "id", "begin", "parent", "dependent", "allowWrites", "readUncommited", "readCommitted",
            "keepAlive", "status", "commit", "globalCommit", "counter");
    Object family;
    Object ageQualifier;
    Object jobQualifier;

    ClientTransactor clientTransactor;
    public Transactor transactor;
    public HTransactor hTransactor;
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
        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, immutableCache, activeCache, cache, 1000);

        transactor = new SITransactor(new SimpleTimestampSource(), dataLib, writer,
                new DataStore(dataLib, reader, writer, "si-needed", SI_NEEDED_VALUE, ONLY_SI_FAMILY_NEEDED_VALUE,
                        "si-transaction-id", "si-delete-put", SNAPSHOT_ISOLATION_FAMILY,
                        SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING, SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING,
                        SNAPSHOT_ISOLATION_PLACE_HOLDER_COLUMN_STRING,
                        -1, -2, userColumnsFamilyName),
                transactionStore, storeSetup.getClock(), 1500);
        if (!simple) {
            hTransactor = new HTransactorAdapter(transactor);
        }
        clientTransactor = transactor;
    }

}
