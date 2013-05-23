package com.splicemachine.si.api.com.splicemachine.si.api.hbase;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SScan;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HDataLibAdapter;
import com.splicemachine.si.data.hbase.HPoolTableSource;
import com.splicemachine.si.data.hbase.HStore;
import com.splicemachine.si.data.hbase.HTableReaderAdapter;
import com.splicemachine.si.data.hbase.HTableSource;
import com.splicemachine.si.data.hbase.HTableWriterAdapter;
import com.splicemachine.si.data.hbase.HTransactorAdapter;
import com.splicemachine.si.impl.ActiveTransactionCacheEntry;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.ImmutableTransaction;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.si.impl.SystemClock;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionSchema;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.si.txn.ZooKeeperTimestampSource;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

import java.util.concurrent.TimeUnit;

public class HTransactorFactory extends SIConstants {
    private static volatile HTransactor transactor;

    public static void setTransactor(HTransactor transactorToUse) {
        transactor = transactorToUse;
    }

    public static HClientTransactor getClientTransactor() {
        return getTransactor();
    }

    public static HTransactor getTransactor() {
        if (transactor == null) {
            synchronized (HTransactorFactory.class) {
                if (transactor == null) {
                    final HTablePool pool = new HTablePool(config, Integer.MAX_VALUE);
                    transactor = createTransactor(new HPoolTableSource(pool), new ZooKeeperTimestampSource(zkSpliceTransactionPath));
                }
            }
        }
        return transactor;
    }

    private static HTransactor createTransactor(HTableSource tableSource, TimestampSource timestampSource) {
        HStore store = new HStore(tableSource);
        SDataLib dataLib = new HDataLibAdapter(new HDataLib());
        final STableReader reader = new HTableReaderAdapter(store);
        final STableWriter writer = new HTableWriterAdapter(store);
        final TransactionSchema transactionSchema = new TransactionSchema(TRANSACTION_TABLE,
                DEFAULT_FAMILY,
                SNAPSHOT_ISOLATION_CHILDREN_FAMILY,
                EMPTY_BYTE_ARRAY,
                TRANSACTION_START_TIMESTAMP_COLUMN,
                TRANSACTION_PARENT_COLUMN_BYTES,
                TRANSACTION_DEPENDENT_COLUMN_BYTES,
                TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
                TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES,
                TRANSACTION_READ_COMMITTED_COLUMN_BYTES,
                TRANSACTION_COMMIT_TIMESTAMP_COLUMN, TRANSACTION_STATUS_COLUMN,
                TRANSACTION_KEEP_ALIVE_COLUMN);
        final Cache<Long, ImmutableTransaction> immutableCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, ActiveTransactionCacheEntry> activeCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final Cache<Long, Transaction> cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
        final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer,
                immutableCache, activeCache, cache, 1000);

        final DataStore rowStore = new DataStore(dataLib, reader, writer, "si-needed", SI_NEEDED_VALUE,
                ONLY_SI_FAMILY_NEEDED_VALUE,
                "si-transaction-id", "si-delete-put", SNAPSHOT_ISOLATION_FAMILY,
                SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING,
                SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING,
                EMPTY_BYTE_ARRAY, SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,
                DEFAULT_FAMILY);
        return new HTransactorAdapter(new SITransactor<Object, SGet, SScan, Mutation, Result>
                        (timestampSource, dataLib, writer, rowStore, transactionStore,
                                new SystemClock(), TRANSACTION_TIMEOUT));
    }

}
