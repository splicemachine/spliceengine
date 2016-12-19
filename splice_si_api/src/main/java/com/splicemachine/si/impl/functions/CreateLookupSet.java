package com.splicemachine.si.impl.functions;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.spark_project.guava.base.Function;

import javax.annotation.Nullable;

/**
 *
 * Utilize Global Cache to resolve transaction and place transactions seen into global cache
 *
 */
public class CreateLookupSet implements Function<UnsafeRow[],long[]> {

    public CreateLookupSet() {

    }


    @Nullable
    @Override
    public long[] apply(UnsafeRow[] iterator) {
        return null;
    }
}
