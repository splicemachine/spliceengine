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
 * Create an Array of elements to act on.
 *
 */
public class BatchFetchActiveRecords implements Function<Iterator<DataCell>,Record[]> {
    public int batch;
    private Record[] records;
    private TxnOperationFactory txnOperationFactory;

    public BatchFetchActiveRecords(int batch, TxnOperationFactory txnOperationFactory) {
        this.batch = batch;
        this.records = new Record[batch];
        this.txnOperationFactory = txnOperationFactory;
    }

    @Nullable
    @Override
    public Record[] apply(Iterator<DataCell> iterator) {
        int i = 0;
        while(iterator.hasNext() && i<batch) {
            records[i] = txnOperationFactory.dataCellToRecord(iterator.next());
            i++;
        }
        if (i!=batch) {
            while (i < batch) {
                records[i] = null;
                i++;
            }
        }
        return records;
    }
}
