package com.splicemachine.storage;

import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.util.MeasuredListScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class RegionResultScanner implements DataResultScanner{
    private final MeasuredListScanner regionScanner;
    private final int batchSize;

    private HResult result = new HResult();
    private List<Cell> list;

    public RegionResultScanner(int batchSize,MeasuredListScanner regionScanner){
        this.regionScanner=regionScanner;
        this.list =new ArrayList<>(batchSize);
        this.batchSize = batchSize;
    }

    @Override
    public DataResult next() throws IOException{
        list.clear();
        regionScanner.next(list,batchSize);
        if(list.size()<=0) return null;

        Result r=Result.create(list);

        result.set(r);
        return result;
    }

    @Override
    public void close() throws IOException{
        regionScanner.close();
    }

    /*Metrics reporting*/
    @Override public TimeView getReadTime(){ return regionScanner.getReadTime(); }
    @Override public long getBytesOutput(){ return regionScanner.getBytesOutput(); }
    @Override public long getRowsFiltered(){ return regionScanner.getRowsFiltered(); }
    @Override public long getRowsVisited(){ return regionScanner.getRowsVisited(); }

}
