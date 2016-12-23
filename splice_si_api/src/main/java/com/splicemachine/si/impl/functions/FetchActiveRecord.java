package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.Record;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.DataCell;
import org.spark_project.guava.base.Function;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 *
 *
 * Fetch the active Record
 *
 */
public class FetchActiveRecord implements Function<DataCell,Record> {
    private Record record;
    private TxnOperationFactory txnOperationFactory;

    public FetchActiveRecord(TxnOperationFactory txnOperationFactory) {
        this.txnOperationFactory = txnOperationFactory;
    }

    @Nullable
    @Override
    public Record apply(DataCell dataCell) {
        return txnOperationFactory.dataCellToRecord(dataCell);
    }
}
