package com.splicemachine.si.coprocessors;

import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner implements InternalScanner {
    private final Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor;
    private final  SICompactionState compactionState;
    private final InternalScanner delegate;
    List<KeyValue> rawList = new ArrayList<KeyValue>();

    public SICompactionScanner(Transactor<IHTable, Put, Get, Scan, Mutation, OperationStatus, Result, KeyValue, byte[], ByteBuffer, Integer> transactor,
                               InternalScanner scanner) {
        this.transactor = transactor;
        this.compactionState = transactor.newCompactionState();
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
    private boolean nextDirect(List<KeyValue> results, int limit) throws IOException {
        rawList.clear();
        final boolean more = (limit == -1) ? delegate.next(rawList) : delegate.next(rawList, limit);
        transactor.compact(compactionState, rawList, results);
        return more;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}