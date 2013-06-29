package com.splicemachine.si.api;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConfiguration;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HPoolTableSource;
import com.splicemachine.si.data.hbase.HRowLock;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.ActiveTransactionCacheEntry;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.ImmutableTransaction;
import com.splicemachine.si.impl.SITransactor;
import com.splicemachine.si.impl.SystemClock;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionSchema;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.si.jmx.ManagedTransactor;
import com.splicemachine.si.jmx.TransactorStatus;
import com.splicemachine.si.txn.ZooKeeperTimestampSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * Used to construct a transactor object that is bound to the HBase types and that provides SI functionality. This is
 * the main entry point for applications using SI. Keeps track of a global, static instance that is shared by all callers.
 * Also defines the various configuration options and plugins for the transactor instance.
 */
public class HTransactorFactory extends SIConstants {
    private final static Cache<Long, ImmutableTransaction> immutableCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
    private final static Cache<Long, ActiveTransactionCacheEntry> activeCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
    private final static Cache<Long, Transaction> cache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
    private final static Cache<Long, Transaction> committedCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();
    private final static Cache<Long, Transaction> failedCache = CacheBuilder.newBuilder().maximumSize(10000).expireAfterWrite(5, TimeUnit.MINUTES).build();

    private static volatile ManagedTransactor managedTransactor;

    public static void setTransactor(ManagedTransactor managedTransactorToUse) {
        managedTransactor = managedTransactorToUse;
    }

    public static TransactorStatus getTransactorStatus() {
        return getTransactorDirect();
    }

    public static TransactorControl getTransactorControl() {
        return getTransactorDirect().getTransactor();
    }

    public static ClientTransactor<Put, Get, Scan, Mutation, byte[]> getClientTransactor() {
        return getTransactorDirect().getTransactor();
    }

    public static Transactor<IHTable, Put, Get, Scan, Mutation, Result, KeyValue, byte[], ByteBuffer> getTransactor() {
        return getTransactorDirect().getTransactor();
    }

    private static ManagedTransactor getTransactorDirect() {
        if (managedTransactor != null) {
            return managedTransactor;
        }
        synchronized (HTransactorFactory.class) {
            //double-checked locking--make sure someone else didn't already create it
            if (managedTransactor != null) {
                return managedTransactor;
            }

            final Configuration configuration = SpliceConfiguration.create();
            TimestampSource timestampSource = new ZooKeeperTimestampSource(zkSpliceTransactionPath);
            HTablePool hTablePool = new HTablePool(configuration, Integer.MAX_VALUE);
            final HPoolTableSource tableSource = new HPoolTableSource(hTablePool);
            SDataLib dataLib = new HDataLib();
            final STableReader reader = new HTableReader(tableSource);
            final STableWriter writer = new HTableWriter();
            final TransactionSchema transactionSchema = new TransactionSchema(TRANSACTION_TABLE,
                    DEFAULT_FAMILY,
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
            managedTransactor = new ManagedTransactor(immutableCache, activeCache, cache);
            final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer,
                    immutableCache, activeCache, cache, committedCache, failedCache, 1000, managedTransactor);

            final DataStore rowStore = new DataStore(dataLib, reader, writer,SI_NEEDED, SI_NEEDED_VALUE,
                    ONLY_SI_FAMILY_NEEDED_VALUE, "si-uncommitted", EMPTY_BYTE_ARRAY, SI_TRANSACTION_ID_KEY,
                    "si-delete-put", SNAPSHOT_ISOLATION_FAMILY, SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_STRING,
                    SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING, EMPTY_BYTE_ARRAY, "zombie",
                    SNAPSHOT_ISOLATION_FAILED_TIMESTAMP, DEFAULT_FAMILY);
            final Transactor transactor = new SITransactor<IHTable, OperationWithAttributes, Mutation, Put, Get, Scan, Result, KeyValue, byte[], ByteBuffer, Delete, HRowLock>
                    (timestampSource, dataLib, writer, rowStore, transactionStore,
                            new SystemClock(), TRANSACTION_TIMEOUT, managedTransactor);
            managedTransactor.setTransactor(transactor);
            HTransactorFactory.setTransactor(HTransactorFactory.managedTransactor);
        }
        return managedTransactor;
    }

}
