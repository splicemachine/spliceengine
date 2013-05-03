package com.splicemachine.si;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.TransactionConstants;
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

public class TransactorSetup {
    final TransactionSchema transactionSchema = new TransactionSchema(TransactionConstants.TRANSACTION_TABLE, "siFamily",
            "siChildrenFamily", -1, "begin", "parent", "dependent", "allowWrites", "readUncommited", "readCommitted",
            "commit", "status", "keepAlive");
    Object family;
    Object ageQualifier;
    Object jobQualifier;
    final String SI_DATA_FAMILY;
    final String SI_DATA_COMMIT_TIMESTAMP_QUALIFIER;

    ClientTransactor clientTransactor;
    public Transactor transactor;
    public final TransactionStore transactionStore;
    public RollForwardQueue rollForwardQueue;

    public TransactorSetup(StoreSetup storeSetup) {
        final SDataLib dataLib = storeSetup.getDataLib();
        final STableReader reader = storeSetup.getReader();
        final STableWriter writer = storeSetup.getWriter();

        final String userColumnsFamilyName = "attributes";
        family = dataLib.encode(userColumnsFamilyName);
        ageQualifier = dataLib.encode("age");
        jobQualifier = dataLib.encode("job");

        final Cache<Long,Transaction> cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long,ImmutableTransaction> immutableCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, cache, immutableCache);

        SI_DATA_FAMILY = "_si";
        SI_DATA_COMMIT_TIMESTAMP_QUALIFIER = "commit";
        SITransactor siTransactor = new SITransactor(new SimpleTimestampSource(), dataLib, writer,
                new DataStore(dataLib, reader, writer, "si-needed", "si-transaction-id", "si-delete-put",
                        SI_DATA_FAMILY, SI_DATA_COMMIT_TIMESTAMP_QUALIFIER, "tombstone", -1, userColumnsFamilyName),
                transactionStore, storeSetup.getClock(), 1500);
        clientTransactor = siTransactor;
        transactor = siTransactor;
    }

}
