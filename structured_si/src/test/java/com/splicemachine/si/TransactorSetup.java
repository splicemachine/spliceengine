package com.splicemachine.si;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.TransactionConstants;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.ImmutableTransaction;
import com.splicemachine.si.impl.SiTransactor;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionSchema;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.SimpleTimestampSource;
import com.splicemachine.si.impl.TransactionStore;

import java.util.concurrent.TimeUnit;

public class TransactorSetup {
    final TransactionSchema transactionSchema = new TransactionSchema(TransactionConstants.TRANSACTION_TABLE, "siFamily",
            "siChildrenFamily",
            "begin", "parent", "dependent", "allowWrites", "readUncommited", "readCommitted", "commit",
            "status");
    Object family;
    Object ageQualifier;
    Object jobQualifier;

    ClientTransactor clientTransactor;
    public Transactor transactor;
    public final TransactionStore transactionStore;

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
        SiTransactor siTransactor = new SiTransactor(new SimpleTimestampSource(), dataLib, writer,
                new DataStore(dataLib, reader, writer, "si-needed", "si-transaction-id", "si-delete-put",
                        "_si", "commit", "tombstone", -1, userColumnsFamilyName),
                transactionStore);
        clientTransactor = siTransactor;
        transactor = siTransactor;
    }

}
