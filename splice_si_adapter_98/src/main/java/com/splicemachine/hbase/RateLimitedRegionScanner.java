package com.splicemachine.hbase;

import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;
import com.splicemachine.metrics.TimeView;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 12/9/14
 */
public class RateLimitedRegionScanner implements MeasuredRegionScanner<Cell> {
    private final MeasuredRegionScanner<Cell> delegate;
    private final TrafficController trafficControl;

    public RateLimitedRegionScanner(MeasuredRegionScanner<Cell> delegate,int readsPerSecond) {
        this.delegate = delegate;
        this.trafficControl = TrafficShaping.fixedRateTrafficShaper(readsPerSecond,readsPerSecond, TimeUnit.SECONDS);
    }

    @Override public void start() { delegate.start(); }
    @Override public TimeView getReadTime() { return delegate.getReadTime(); }
    @Override public long getBytesOutput() { return delegate.getBytesOutput(); }
    @Override public Cell next() throws IOException { return delegate.next(); }
    @Override public long getBytesVisited() { return delegate.getBytesVisited(); }
    @Override public long getRowsOutput() { return delegate.getRowsOutput(); }
    @Override public long getRowsFiltered() { return delegate.getRowsFiltered(); }
    @Override public long getRowsVisited() { return delegate.getRowsVisited(); }
    @Override public HRegionInfo getRegionInfo() { return delegate.getRegionInfo(); }
    @Override public boolean isFilterDone() throws IOException { return delegate.isFilterDone(); }
    @Override public boolean reseek(byte[] row) throws IOException { return delegate.reseek(row); }
    @Override public long getMaxResultSize() { return delegate.getMaxResultSize(); }
    @Override public long getMvccReadPoint() { return delegate.getMvccReadPoint(); }
    @Override public void close() throws IOException { delegate.close(); }

    @Override
    public boolean internalNextRaw(List<Cell> results) throws IOException {
        acquire();
        return delegate.internalNextRaw(results);
    }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        acquire();
        return delegate.nextRaw(result);
    }

    @Override
    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        acquire();
        return delegate.nextRaw(result, limit);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        acquire();
        return delegate.next(results);
    }

    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
        acquire();
        return delegate.next(result, limit);
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
