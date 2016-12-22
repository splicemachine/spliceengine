package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.Record;
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
public class FetchActiveRecords implements Function<Iterator<DataCell>,Record[]> {
    public static Integer fetchNumber = 100;

    @Nullable
    @Override
    public Record[] apply(Iterator<DataCell> iterator) {
        return null;
    }
}
