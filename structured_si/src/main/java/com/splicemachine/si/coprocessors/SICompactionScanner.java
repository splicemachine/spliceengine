package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner implements InternalScanner {
    private final  SICompactionState compactionState;
    private final InternalScanner delegate;
    List<KeyValue> rawList = new ArrayList<KeyValue>();

    public SICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner) {
        this.compactionState = compactionState;
        this.delegate = scanner;
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
        return nextDirect(results, limit);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit, String metric) throws IOException {
        return nextDirect(results, limit);
    }

    /**
     * Read data from the underlying scanner and send the results through the SICompactionState.
     */
    @SuppressWarnings("unchecked")
		private boolean nextDirect(List<KeyValue> results, int limit) throws IOException {
        rawList.clear();
        final boolean more = delegate.next(rawList);
		compactionState.mutate(rawList, results);
		return more;
    }

    @Override
    public void close() throws IOException {
//    	compactionState.close();
        delegate.close();
    }
}