package com.splicemachine.si.impl.functions;

import com.google.common.base.Function;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.storage.Record;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 *
 *
 * Create an Array of elements to act on.
 *
 */
public class BatchFetchActiveRecords implements Function<Iterator<Record>,Record[]> {
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
    public Record[] apply(Iterator<Record> iterator) {
        int i = 0;
        while(iterator.hasNext() && i<batch) {
            records[i] = iterator.next();
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
