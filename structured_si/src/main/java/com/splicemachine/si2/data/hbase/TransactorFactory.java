package com.splicemachine.si2.data.hbase;

import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.RowMetadataStore;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;

public class TransactorFactory {

    public static Transactor newTransactorForFiltering() {
        HStore store = new HStore(null);
        SDataLib dataLib = new HDataLibAdapter(new HDataLib());
        final STableReader reader = new HTableReaderAdapter(store);
        final STableWriter writer = new HTableWriterAdapter(store);
        final TransactionSchema transactionSchema = new TransactionSchema("transaction", "siFamily", "begin", "commit", "status");
        final TransactionStore transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer);

        final RowMetadataStore rowStore = new RowMetadataStore(dataLib, reader, writer, "si-needed", "_si", "commit", -1, "attributes");
        return new SiTransactor(null, dataLib, writer, rowStore, transactionStore);
    }
}
