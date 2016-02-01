package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.util.MeasuredResultScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
@NotThreadSafe
public class ListingResultScanner implements DataScanner{
    private final MeasuredResultScanner resultScanner;
    private final Partition partition;

    private final HCell wrapper = new HCell();
    private final Function<Cell, DataCell> wrapFunction=new Function<Cell, DataCell>(){
        @Override
        public DataCell apply(Cell input){
            wrapper.set(input);
            return wrapper;
        }
    };

    public ListingResultScanner(Partition table, MeasuredResultScanner resultScanner){
        this.resultScanner = resultScanner;
        this.partition = table;
    }

    @Override
    @Nonnull
    public List<DataCell> next(int limit) throws IOException{
        Result r = resultScanner.next();
        if(r==null||r.size()<=0) return Collections.emptyList();
        List<Cell> cells=r.listCells();
        return Lists.transform(cells,wrapFunction); //TODO -sf- this uses 1 extra object, maybe avoid that with a fixed list?
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
