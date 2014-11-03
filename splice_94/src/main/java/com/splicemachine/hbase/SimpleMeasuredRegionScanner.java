package com.splicemachine.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/4/14
 */
public class SimpleMeasuredRegionScanner implements MeasuredRegionScanner<KeyValue> {
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
    public KeyValue next() throws IOException {
        List<KeyValue> kvs = Lists.newArrayList();
        nextRaw(kvs,null);
        return kvs.get(0);
    }

    @Override public long getBytesVisited() { return getBytesOutput(); }
    @Override public long getRowsOutput() { return readTimer.getNumEvents(); }
    @Override public long getRowsFiltered() { return 0l; }
    @Override public long getRowsVisited() { return getRowsOutput(); }
    @Override public HRegionInfo getRegionInfo() { return delegate.getRegionInfo(); }
    @Override public boolean isFilterDone() { return delegate.isFilterDone(); }
    @Override public boolean reseek(byte[] row) throws IOException { return delegate.reseek(row); }
    @Override public long getMvccReadPoint() { return delegate.getMvccReadPoint(); }

    @Override
    public boolean nextRaw(List<KeyValue> result, String metric) throws IOException {
        return nextRaw(result,Integer.MAX_VALUE,metric);
    }

    @Override
    public boolean nextRaw(List<KeyValue> result, int limit, String metric) throws IOException {
        readTimer.startTiming();
        boolean shouldContinue = delegate.nextRaw(result,limit,metric);
        if(result.size()>0){
            readTimer.tick(1l);
            if(bytesCounter.isActive()){
                for(KeyValue kv:result){
                    bytesCounter.add(kv.getLength());
                }
            }
        }else
            readTimer.stopTiming();
        return shouldContinue;
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        return nextRaw(results,Integer.MAX_VALUE,null);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
        return nextRaw(results,Integer.MAX_VALUE,metric);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
        return nextRaw(result,limit,null);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit, String metric) throws IOException {
        return nextRaw(result,limit,metric);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
