package com.splicemachine.hbase;

import com.splicemachine.si.impl.server.SICompactionState;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner implements InternalScanner {
    private final SICompactionState compactionState;
    private final InternalScanner delegate;
    private List<Cell> rawList =new ArrayList<>();

    public SICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner) {
        this.compactionState = compactionState;
        this.delegate = scanner;
    }

    @Override
    public boolean next(List<Cell> list) throws IOException{
        /*
         * Read data from the underlying scanner and send the results through the SICompactionState.
         */
        rawList.clear();
        final boolean more = delegate.next(rawList);
        compactionState.mutate(rawList, list);
        return more;
    }

    public boolean next(List<Cell> results, int limit) throws IOException {
        return next(results);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}