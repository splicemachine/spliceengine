package com.splicemachine.si.impl.functions;

import com.google.common.base.Function;
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

    public BatchFetchActiveRecords(int batch) {
        this.batch = batch;
        this.records = new Record[batch];
    }

    @Nullable
    @Override
    public Record[] apply(Iterator<Record> iterator) {
        if (iterator == null)
            return null;
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
