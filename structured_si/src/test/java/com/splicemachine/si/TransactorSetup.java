package com.splicemachine.si;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.data.hbase.HTransactor;
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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

import java.util.concurrent.TimeUnit;

public class TransactorSetup extends SIConstants {
    final TransactionSchema transactionSchema = new TransactionSchema(SpliceConstants.TRANSACTION_TABLE, "siFamily",
            "siChildrenFamily", -1, "begin", "parent", "dependent", "allowWrites", "readUncommited", "readCommitted",
            "commit", "status", "keepAlive");
    Object family;
    Object ageQualifier;
    Object jobQualifier;

    ClientTransactor clientTransactor;
    public Transactor transactor;
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

        final Cache<Long, Transaction> cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, ImmutableTransaction> immutableCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, cache, immutableCache);

        transactor = new SITransactor(new SimpleTimestampSource(), dataLib, writer,
                new DataStore(dataLib, reader, writer, "si-needed", SI_NEEDED_VALUE, ONLY_SI_FAMILY_NEEDED_VALUE,
                        "si-transaction-id", "si-delete-put", SNAPSHOT_ISOLATION_FAMILY,
                        SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING, SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING,
                        -1, -2, userColumnsFamilyName),
                transactionStore, storeSetup.getClock(), 1500);
        if (!simple) {
            transactor = new TransactorAdapter(new HTransactor<Put, Get, Scan, Mutation>(transactor));
        }
        clientTransactor = transactor;
    }

}
