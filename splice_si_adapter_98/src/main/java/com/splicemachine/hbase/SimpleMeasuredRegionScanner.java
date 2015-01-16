package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/4/14
 */
public class SimpleMeasuredRegionScanner implements MeasuredRegionScanner<Cell> {
    private final RegionScanner delegate;
    private Timer readTimer;
    private Counter bytesCounter;


    public SimpleMeasuredRegionScanner(RegionScanner delegate, MetricFactory metricFactory) {
        this.delegate = delegate;
        this.readTimer = metricFactory.newTimer();
        this.bytesCounter = metricFactory.newCounter();
    }

    @Override public void start() {  }

    @Override public TimeView getReadTime() { return readTimer.getTime(); }
    @Override public long getBytesOutput() { return bytesCounter.getTotal(); }

    @Override
    public Cell next() throws IOException {
        List<Cell> kvs = Lists.newArrayList();
        nextRaw(kvs);
        return kvs.get(0);
    }

    @Override public long getBytesVisited() { return getBytesOutput(); }
    @Override public long getRowsOutput() { return readTimer.getNumEvents(); }
    @Override public long getRowsFiltered() { return 0l; }
    @Override public long getRowsVisited() { return getRowsOutput(); }
    @Override public HRegionInfo getRegionInfo() { return delegate.getRegionInfo(); }
    @Override public boolean isFilterDone() throws IOException { return delegate.isFilterDone(); }
    @Override public boolean reseek(byte[] row) throws IOException { return delegate.reseek(row); }
    @Override public long getMvccReadPoint() { return delegate.getMvccReadPoint(); }

    @Override
    public boolean nextRaw(List<Cell> result) throws IOException {
        return nextRaw(result,Integer.MAX_VALUE);
    }

    @Override
    public boolean nextRaw(List<Cell> result, int limit) throws IOException {
        readTimer.startTiming();
        boolean shouldContinue = delegate.nextRaw(result,limit);
        if(result.size()>0){
            readTimer.tick(1l);
            if(bytesCounter.isActive()){
                for(Cell kv:result){
                    bytesCounter.add(kv.getRowLength() +kv.getValueLength());
                }
            }
        }else
            readTimer.stopTiming();
        return shouldContinue;
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return nextRaw(results,Integer.MAX_VALUE);
    }

    @Override
    public boolean next(List<Cell> result, int limit) throws IOException {
        return nextRaw(result,limit);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

	@Override
	public long getMaxResultSize() {
		return delegate.getMaxResultSize();
	}

	@Override
	public boolean internalNextRaw(List<Cell> results) throws IOException {
		return delegate.nextRaw(results);
	}
}
