package com.splicemachine.si.api.com.splicemachine.si.api.hbase;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.HbaseConfigurationSource;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HTransactorFactory extends SIConstants {
    private static HTransactor defaultTransactor;
    private static volatile HTablePool hTablePool;

    public static void setTransactor(HTransactor transactorToUse) {
        defaultTransactor = transactorToUse;
        transactor = transactorToUse;
    }

    public static HClientTransactor getClientTransactor() {
        return defaultTransactor;
    }

    public static HTransactor getTransactor(Configuration configuration, TimestampSource timestampSource) {
        if (hTablePool == null) {
            synchronized (HTransactorFactory.class) {
                if (hTablePool == null) {
                    hTablePool = new HTablePool(configuration, Integer.MAX_VALUE);
                }
            }
        }
        return getTransactor(hTablePool, timestampSource);
    }

    public static HTransactor getTransactor(HTablePool pool, TimestampSource timestampSource) {
        return getTransactorDirect(new HPoolTableSource(pool), timestampSource);
    }

    public static HTransactor getTransactorDirect(HTableSource tableSource, TimestampSource timestampSource) {
        HStore store = new HStore(tableSource);
        SDataLib dataLib = new HDataLibAdapter(new HDataLib());
        final STableReader reader = new HTableReaderAdapter(store);
        final STableWriter writer = new HTableWriterAdapter(store);
        final TransactionSchema transactionSchema = new TransactionSchema(TRANSACTION_TABLE,
                DEFAULT_FAMILY,
                SNAPSHOT_ISOLATION_CHILDREN_FAMILY,
                EMPTY_BYTE_ARRAY,
                TRANSACTION_ID_COLUMN,
                TRANSACTION_START_TIMESTAMP_COLUMN,
                TRANSACTION_PARENT_COLUMN_BYTES,
                TRANSACTION_DEPENDENT_COLUMN_BYTES,
                TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
                TRANSACTION_READ_UNCOMMITTED_COLUMN_BYTES,
                TRANSACTION_READ_COMMITTED_COLUMN_BYTES,
                TRANSACTION_KEEP_ALIVE_COLUMN, TRANSACTION_STATUS_COLUMN,
                TRANSACTION_COMMIT_TIMESTAMP_COLUMN,
                TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN,
                TRANSACTION_COUNTER_COLUMN
        );
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


    private static volatile HTransactor transactor;
    //deliberate boxing here to ensure the lock is not shared by anyone else
    private static final Object lock = new Integer(1);

    public HTransactor newTransactor(HbaseConfigurationSource configSource) throws IOException {
        return getTransactor(configSource);
    }

    public static HTransactor getTransactor() {
        return getTransactor(new HbaseConfigurationSource() {
            @Override
            public Configuration getConfiguration() {
                return config;
            }
        });
    }

    public static HTransactor getTransactor(HbaseConfigurationSource configSource) {
        if(transactor!=null) return transactor;
        synchronized (lock) {
            //double-checked locking--make sure someone else didn't already create it
            if(transactor!=null)
                return transactor;

            final Configuration configuration = configSource.getConfiguration();
//            TransactionTableCreator.createTransactionTableIfNeeded(configuration);
            TimestampSource timestampSource = new ZooKeeperTimestampSource(zkSpliceTransactionPath);
            transactor = HTransactorFactory.getTransactor(configuration, timestampSource);
            HTransactorFactory.setTransactor(transactor);
        }
        return transactor;
    }

}
