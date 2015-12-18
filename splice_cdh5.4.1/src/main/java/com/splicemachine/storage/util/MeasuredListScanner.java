package com.splicemachine.storage.util;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;


/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class MeasuredListScanner implements RegionScanner{
    private final RegionScanner delegate;
    private final Timer timer;
    private final Counter filterCounter;
    private final Counter outputBytesCounter;

    public MeasuredListScanner(RegionScanner delegate,MetricFactory metricFactory){
        this.delegate=delegate;
        this.timer = metricFactory.newTimer();
        this.filterCounter = metricFactory.newCounter();
        this.outputBytesCounter = metricFactory.newCounter();
    }

    @Override public HRegionInfo getRegionInfo(){ return delegate.getRegionInfo(); }
    @Override public boolean isFilterDone() throws IOException{ return delegate.isFilterDone(); }
    @Override public boolean reseek(byte[] bytes) throws IOException{ return delegate.reseek(bytes); }
    @Override public long getMaxResultSize(){ return delegate.getMaxResultSize(); }
    @Override public long getMvccReadPoint(){ return delegate.getMvccReadPoint(); }

    @Override
    public boolean nextRaw(List<Cell> list) throws IOException{
        timer.startTiming();
        boolean b=delegate.nextRaw(list);
        timer.tick(list.size()>0?1l:0l);

        if(outputBytesCounter.isActive())
            countOutput(list);
        return b;
    }

    @Override
    public boolean nextRaw(List<Cell> list,int limit) throws IOException{
        //TODO -sf- do not ignore the limit
        return nextRaw(list);
    }

    @Override
    public boolean next(List<Cell> list) throws IOException{
        return nextRaw(list);
    }

    @Override
    public boolean next(List<Cell> list,int limit) throws IOException{
        return nextRaw(list,limit);
    }

    @Override
    public void close() throws IOException{
        delegate.close();
    }

    /*Metrics reporting*/
    public TimeView getReadTime(){ return timer.getTime(); }
    public long getBytesOutput(){ return outputBytesCounter.getTotal(); }
    public long getRowsFiltered(){ return filterCounter.getTotal(); }
    public long getRowsVisited(){ return timer.getNumEvents(); }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void countOutput(List<Cell> list){
        //TODO -sf- count the output
    }
}
