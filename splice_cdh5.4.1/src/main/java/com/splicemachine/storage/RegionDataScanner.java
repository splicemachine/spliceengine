package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataScanner;
import com.splicemachine.storage.HCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public class RegionDataScanner implements DataScanner{
    private final Timer readTimer;
    private final Counter outputBytesCounter;
    private final Counter filteredRowCounter;
    private final Counter visitedRowCounter;
    private final RegionScanner delegate;

    private List<Cell> internalList;

    private final HCell cell = new HCell();

    private final Function<Cell,DataCell> transform = new Function<Cell, DataCell>(){
        @Override
        public DataCell apply(Cell input){
            cell.set(input);
            return cell;
        }
    };

    public RegionDataScanner(RegionScanner delegate,MetricFactory metricFactory){
        this.delegate=delegate;
        this.readTimer = metricFactory.newTimer();
        this.outputBytesCounter = metricFactory.newCounter();
        this.filteredRowCounter = metricFactory.newCounter();
        this.visitedRowCounter = metricFactory.newCounter();
    }

    @Override
    public List<DataCell> next(int limit) throws IOException{
        if(internalList!=null)
            internalList = new ArrayList<>(limit>0?limit:10);
        readTimer.startTiming();
        delegate.next(internalList);
        readTimer.stopTiming();
        collectMetrics(internalList);

        return Lists.transform(internalList,transform);
    }


    @Override public TimeView getReadTime(){ return readTimer.getTime(); }
    @Override public long getBytesOutput(){ return outputBytesCounter.getTotal(); }
    @Override public long getRowsFiltered(){ return filteredRowCounter.getTotal(); }
    @Override public long getRowsVisited(){ return visitedRowCounter.getTotal(); }

    @Override public void close() throws IOException{ delegate.close(); }


    /* *********************************************************************************************************/
    /*private helper methods*/
    private void collectMetrics(List<Cell> internalList){
        //TODO -sf- implement!
    }
}
