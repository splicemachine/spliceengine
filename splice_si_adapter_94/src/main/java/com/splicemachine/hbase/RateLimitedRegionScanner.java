package com.splicemachine.hbase;

import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;
import com.splicemachine.metrics.TimeView;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 12/9/14
 */
public class RateLimitedRegionScanner implements MeasuredRegionScanner<KeyValue> {
    private final MeasuredRegionScanner<KeyValue> delegate;
    private final TrafficController trafficControl;

    public RateLimitedRegionScanner(MeasuredRegionScanner<KeyValue> delegate,int readsPerSecond) {
        this.delegate = delegate;
        this.trafficControl = TrafficShaping.fixedRateTrafficShaper(readsPerSecond,readsPerSecond, TimeUnit.SECONDS);
    }

    @Override public void start() { delegate.start(); }
    @Override public TimeView getReadTime() { return delegate.getReadTime(); }
    @Override public long getBytesOutput() { return delegate.getBytesOutput(); }
    @Override public long getBytesVisited() {  return delegate.getBytesVisited(); }
    @Override public long getRowsOutput() { return delegate.getRowsOutput(); }
    @Override public long getRowsFiltered() { return delegate.getRowsFiltered(); }
    @Override public long getRowsVisited() { return delegate.getRowsVisited(); }
    @Override public HRegionInfo getRegionInfo() { return delegate.getRegionInfo(); }
    @Override public boolean isFilterDone() { return delegate.isFilterDone(); }
    @Override public boolean reseek(byte[] bytes) throws IOException { return delegate.reseek(bytes); }
    @Override public long getMvccReadPoint() { return delegate.getMvccReadPoint(); }
    @Override public void close() throws IOException { delegate.close(); }

    @Override
    public KeyValue next() throws IOException {
        acquire();
        return delegate.next();
    }

    @Override
    public boolean internalNextRaw(List<KeyValue> results) throws IOException {
        acquire();
        return delegate.internalNextRaw(results);
    }


    @Override
    public boolean nextRaw(List<KeyValue> list, String s) throws IOException {
        acquire();
        return delegate.nextRaw(list, s);
    }

    @Override
    public boolean nextRaw(List<KeyValue> list, int i, String s) throws IOException {
        acquire();
        return delegate.nextRaw(list, i, s);
    }

    @Override
    public boolean next(List<KeyValue> list) throws IOException {
        acquire();
        return delegate.next(list);
    }

    @Override
    public boolean next(List<KeyValue> list, String s) throws IOException {
        acquire();
        return delegate.next(list, s);
    }

    @Override
    public boolean next(List<KeyValue> list, int i) throws IOException {
        acquire();
        return delegate.next(list, i);
    }

    @Override
    public boolean next(List<KeyValue> list, int i, String s) throws IOException {
        acquire();
        return delegate.next(list, i, s);
    }


    /*****************************************************************************************************************/
    /*private helper methods*/
    private void acquire() throws InterruptedIOException {
        try {
            trafficControl.acquire(1);
        } catch (InterruptedException e) {
            throw new InterruptedIOException(e.getMessage());
        }
    }
}

