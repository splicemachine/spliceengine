package com.splicemachine.storage;

import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.util.MeasuredResultScanner;
import org.apache.hadoop.hbase.client.Result;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
@NotThreadSafe
public class ResultDataScanner implements DataResultScanner{
    private final MeasuredResultScanner resultScanner;

    private HResult wrapper = new HResult();
    public ResultDataScanner(MeasuredResultScanner resultScanner){
        this.resultScanner=resultScanner;
    }

    @Override
    public DataResult next() throws IOException{
        Result next=resultScanner.next();
        if(next==null||next.size()<=0) {
            return null;
        }

        wrapper.set(next);
        return wrapper;
    }

    @Override
    public void close() throws IOException{
        resultScanner.close();
    }

    /*Metrics reporting*/
    @Override public TimeView getReadTime(){ return resultScanner.getTime(); }
    @Override public long getBytesOutput(){ return resultScanner.getBytesOutput(); }
    @Override public long getRowsFiltered(){ return resultScanner.getRowsFiltered(); }
    @Override public long getRowsVisited(){ return resultScanner.getRowsVisited(); }
}
