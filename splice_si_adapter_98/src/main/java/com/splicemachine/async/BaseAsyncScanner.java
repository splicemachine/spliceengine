package com.splicemachine.async;

import com.splicemachine.collections.NullStopIterator;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.stream.BaseCloseableStream;
import com.splicemachine.stream.CloseableStream;
import com.splicemachine.stream.StreamException;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/15/14
 */
public abstract class BaseAsyncScanner implements AsyncScanner{
    protected final Timer timer;
    protected final com.splicemachine.metrics.Counter remoteBytesCounter;

    public BaseAsyncScanner(MetricFactory metricFactory) {
        this.timer = metricFactory.newTimer();
        this.remoteBytesCounter = metricFactory.newCounter();
    }

    @Override
    public CloseableStream<List<KeyValue>> stream() {
        return new BaseCloseableStream<List<KeyValue>>() {
            @Override
            public void close() throws IOException {
                BaseAsyncScanner.this.close();
            }

            @Override
            public List<KeyValue> next() throws StreamException {
                try {
                    return nextKeyValues();
                } catch (Exception e) {
                    throw new StreamException(e);
                }
            }
        };
    }

    @Override public TimeView getRemoteReadTime() { return timer.getTime(); }
    @Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
    @Override public long getRemoteRowsRead() { return timer.getNumEvents(); }
    @Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
    @Override public long getLocalBytesRead() { return 0l; }
    @Override public long getLocalRowsRead() { return 0l; }

    @Override
    public Result next() throws IOException {
        try {
            List<KeyValue> kvs = nextKeyValues();
            if(kvs!=null && kvs.size()>0)
                return new Result(AsyncScannerUtils.convertFromAsync(kvs));
            return null;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public Result[] next(int size) throws IOException {
        Result[] results = new Result[size];
        Result next;
        int i=0;
        while(i<size && (next = next())!=null){
            results[i] = next;
            i++;
        }
        return results;
    }

    @Override
    public Iterator<Result> iterator() {
        return new NullStopIterator<Result>() {
            @Override public void close() throws IOException { BaseAsyncScanner.this.close();  }
            @Override protected Result nextItem() throws IOException { return BaseAsyncScanner.this.next(); }
        };
    }
}
