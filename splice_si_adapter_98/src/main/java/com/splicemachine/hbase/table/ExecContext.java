package com.splicemachine.hbase.table;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Pair;

import java.util.List;

/**
 * Created by jleach on 7/14/15.
 */
public class ExecContext {
    public final Pair<byte[], byte[]> keyBoundary;
    public final List<Throwable> errors;
    public int attemptCount = 0;

    public ExecContext(Pair<byte[], byte[]> keyBoundary) {
        this.keyBoundary = keyBoundary;
        this.errors = Lists.newArrayListWithExpectedSize(0);
    }

    public ExecContext(Pair<byte[], byte[]> keys, List<Throwable> errors, int attemptCount) {
        this.keyBoundary = keys;
        this.errors = errors;
        this.attemptCount = attemptCount;
    }
}
