package com.splicemachine.si.coprocessors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.InternalScanner;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.SICompactionState;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner implements InternalScanner {
    private final Transactor<IHTable, Mutation,Put> transactor;
    private final  SICompactionState compactionState;
    private final InternalScanner delegate;
    List<Cell> rawList = new ArrayList<Cell>();

    public SICompactionScanner(Transactor<IHTable, Mutation,Put> transactor,
                               InternalScanner scanner) {
        this.transactor = transactor;
        this.compactionState = transactor.newCompactionState();
        this.delegate = scanner;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<Cell> results, int limit) throws IOException {
        return nextDirect(results, limit);
    }

    /**
     * Read data from the underlying scanner and send the results through the SICompactionState.
     */
    @SuppressWarnings("unchecked")
		private boolean nextDirect(List<Cell> results, int limit) throws IOException {
        rawList.clear();
        final boolean more = delegate.next(rawList);
		compactionState.mutate(rawList, results);
        return more;
    }

    @Override
    public void close() throws IOException {
    	compactionState.close();
        delegate.close();
    }
}