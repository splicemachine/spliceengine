package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.ActiveConglomerate;
import com.splicemachine.storage.DataCell;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.spark_project.guava.base.Function;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 *
 *
 * Create an Array of elements to act on.
 *
 */
public class FetchActiveRecords implements Function<Iterator<DataCell>,ActiveConglomerate[]> {
    public static Integer fetchNumber = 100;

    @Nullable
    @Override
    public ActiveConglomerate[] apply(Iterator<DataCell> iterator) {
        return null;
    }
}
