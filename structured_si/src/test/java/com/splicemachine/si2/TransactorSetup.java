package com.splicemachine.si2;

import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si2.data.api.SDataLib;
import com.splicemachine.si2.data.api.STableReader;
import com.splicemachine.si2.data.api.STableWriter;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.si2.si.impl.RowMetadataStore;
import com.splicemachine.si2.si.impl.SiTransactor;
import com.splicemachine.si2.si.impl.SimpleTimestampSource;
import com.splicemachine.si2.si.impl.TransactionSchema;
import com.splicemachine.si2.si.impl.TransactionStore;

public class TransactorSetup {
    final TransactionSchema transactionSchema = new TransactionSchema(SIConstants.TRANSACTION_TABLE, "siFamily", "begin", "commit", "status");
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

        transactionStore = new TransactionStore(transactionSchema, dataLib, reader, writer);
        SiTransactor siTransactor = new SiTransactor(new SimpleTimestampSource(), dataLib, writer,
                new RowMetadataStore(dataLib, reader, writer, "si-needed", "si-transaction-id",
                        "_si", "commit", "tombstone", -1, userColumnsFamilyName),
                transactionStore);
        clientTransactor = siTransactor;
        transactor = siTransactor;
    }

}
