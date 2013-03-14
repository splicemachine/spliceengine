package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.RowMetadataStore;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.HTablePool;

public class TransactorFactory {
    private static Transactor transactor;

    public static void setTransactor(Transactor transactorToUse) {
        transactor = transactorToUse;
    }

    public static Transactor getTransactor() {
        return getTransactorDirect(null);
    }

    public static ClientTransactor getClientTransactor() {
        return (ClientTransactor) getTransactorDirect(null);
    }

    public static Transactor getTransactor(HTablePool pool) {
        return getTransactorDirect(new HPoolTableSource(pool));
    }

    public static Transactor getTransactor(CoprocessorEnvironment environment) {
        return getTransactorDirect(new HCoprocessorTableSource(environment));
    }

    public static Transactor getTransactorDirect(HTableSource tableSource) {
        if (transactor == null) {
            HStore store = new HStore(tableSource);
            SDataLib dataLib = new HDataLibAdapter(new HDataLib());
            final STableReader reader = new HTableReaderAdapter(store);
            final STableWriter writer = new HTableWriterAdapter(store);
            final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "begin", "commit", "status");
            final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer);

            final RowMetadataStore rowStore = new RowMetadataStore(dataLib, reader, writer, "si-needed",
                    "si-transaction-id", "_si", "commit", -1, "attributes");
            return new SiTransactor(null, dataLib, writer, rowStore, transactionStore);
        } else {
            return transactor;
        }
    }
}
