package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.KeyValue;
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

    public SICompactionScanner(Transactor transactor, InternalScanner scanner) {
        this.compactionState = transactor.newCompactionState();
        this.delegate = scanner;
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
        return nextDirect(results, limit);
    }

    /**
     * Read data from the underlying scanner and send the results through the SICompactionState.
     */
    private boolean nextDirect(List<KeyValue> results, int limit) throws IOException {
        List<KeyValue> rawList = new ArrayList<KeyValue>((limit == -1) ? 0 : limit);
        final boolean more = (limit == -1) ? delegate.next(rawList) : delegate.next(rawList, limit);
        compactionState.mutate(rawList, results);
        return more;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}