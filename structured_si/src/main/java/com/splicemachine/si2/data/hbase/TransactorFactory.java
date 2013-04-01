package com.splicemachine.si2.data.hbase;

import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.TimestampSource;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.DataStore;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTablePool;

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
                SIConstants.DEFAULT_FAMILY, SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN,
                SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN_BYTES,
                SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN, SIConstants.TRANSACTION_STATUS_COLUMN);
        final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer);

        final DataStore rowStore = new DataStore(dataLib, reader, writer, "si-needed",
                "si-transaction-id", "si-delete-put", SIConstants.SNAPSHOT_ISOLATION_FAMILY,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN,
                SIConstants.EMPTY_BYTE_ARRAY, SIConstants.DEFAULT_FAMILY);
        return new SiTransactor(timestampSource, dataLib, writer, rowStore, transactionStore);
    }
}
