package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.util.MeasuredResultScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
@NotThreadSafe
public class ListingResultScanner implements DataScanner{
    private final MeasuredResultScanner resultScanner;
    private final Partition partition;

    private HCell wrapper = new HCell();

    public ListingResultScanner(Partition table, MeasuredResultScanner resultScanner){
        this.resultScanner = resultScanner;
        this.partition = table;
    }

    @Override
    public List<DataCell> next(int limit) throws IOException{
        Result r = resultScanner.next();
        List<Cell> cells=r.listCells();
        return Lists.transform(cells,new Function<Cell, DataCell>(){
            @Override
            public DataCell apply(Cell input){
                wrapper.set(input);
                return wrapper;
            }
        });
    }
    @Override
    public void close() throws IOException{
        resultScanner.close();
    }

    @Override
    public Partition getPartition(){
        return partition;
    }

    /*Metrics reporting*/
    @Override public TimeView getReadTime(){ return resultScanner.getTime(); }
    @Override public long getBytesOutput(){ return resultScanner.getBytesOutput(); }
    @Override public long getRowsFiltered(){ return resultScanner.getRowsFiltered(); }
    @Override public long getRowsVisited(){ return resultScanner.getRowsVisited(); }
}
