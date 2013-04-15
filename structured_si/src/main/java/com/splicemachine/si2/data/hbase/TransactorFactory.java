package com.splicemachine.si2.data.hbase;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.si2.impl.SiTransactor;
import com.splicemachine.si2.impl.TransactionSchema;
import com.splicemachine.si2.impl.TransactionStruct;
import com.splicemachine.si2.utils.SIConstants;
import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.api.ClientTransactor;
import com.splicemachine.si2.api.TimestampSource;
import com.splicemachine.si2.api.Transactor;
import com.splicemachine.si2.impl.DataStore;
import com.splicemachine.si2.impl.ImmutableTransactionStruct;
import com.splicemachine.si2.impl.TransactionStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTablePool;

import java.util.concurrent.TimeUnit;

public class TransactorFactory {
    private static Transactor defaultTransactor;
    private static volatile HTablePool hTablePool;

    public static void setDefaultTransactor(Transactor transactorToUse) {
        defaultTransactor = transactorToUse;
    }

    public static Transactor getDefaultTransactor() {
        return defaultTransactor;
    }

    public static ClientTransactor getDefaultClientTransactor() {
        return (ClientTransactor) defaultTransactor;
    }

    public static Transactor getTransactor(Configuration configuration, TimestampSource timestampSource) {
        if (hTablePool == null) {
            synchronized (TransactorFactory.class) {
                if (hTablePool == null) {
                    hTablePool = new HTablePool(configuration, Integer.MAX_VALUE);
                }
            }
        }
        return getTransactor(hTablePool, timestampSource);
    }

    public static Transactor getTransactor(HTablePool pool, TimestampSource timestampSource) {
        return getTransactorDirect(new HPoolTableSource(pool), timestampSource);
    }

    public static Transactor getTransactor(CoprocessorEnvironment environment, TimestampSource timestampSource) {
        return getTransactorDirect(new HCoprocessorTableSource(environment), timestampSource);
    }

    public static Transactor getTransactorDirect(HTableSource tableSource, TimestampSource timestampSource) {
        HStore store = new HStore(tableSource);
        SDataLib dataLib = new HDataLibAdapter(new HDataLib());
        final STableReader reader = new HTableReaderAdapter(store);
        final STableWriter writer = new HTableWriterAdapter(store);
        final TransactionSchema transactionSchema = new TransactionSchema(SIConstants.TRANSACTION_TABLE,
                SIConstants.DEFAULT_FAMILY,
                SIConstants.SNAPSHOT_ISOLATION_CHILDREN_FAMILY,
                SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN,
                SIConstants.TRANSACTION_PARENT_COLUMN_BYTES,
                SIConstants.TRANSACTION_DEPENDENT_COLUMN_BYTES,
                SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
                SIConstants.TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES,
                SIConstants.TRANSACTION_READ_COMMITTED_COLUMN_BYTES,
                SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN, SIConstants.TRANSACTION_STATUS_COLUMN);
        final Cache<Long,TransactionStruct> cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long,ImmutableTransactionStruct> immutableCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer, cache,
                immutableCache);

        final DataStore rowStore = new DataStore(dataLib, reader, writer, "si-needed",
                "si-transaction-id", "si-delete-put", SIConstants.SNAPSHOT_ISOLATION_FAMILY,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN,
                SIConstants.EMPTY_BYTE_ARRAY, SIConstants.DEFAULT_FAMILY);
        return new SiTransactor(timestampSource, dataLib, writer, rowStore, transactionStore);
    }
}
