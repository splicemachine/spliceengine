package com.splicemachine.pipeline.writehandler;

import com.splicemachine.derby.hbase.StatisticsWatcher;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 4/7/15
 */
public class StatisticsWriteHandler implements WriteHandler{
    private final StatisticsWatcher statisticsWatcher;

    public StatisticsWriteHandler(StatisticsWatcher statisticsWatcher){
        this.statisticsWatcher=statisticsWatcher;
    }

    //no-ops mostly
    @Override public void next(KVPair mutation,WriteContext ctx){  }
    @Override public void next(List<KVPair> mutations,WriteContext ctx){  }
    @Override public void flush(WriteContext ctx) throws IOException{  }

    @Override
    public void close(WriteContext ctx) throws IOException{
        //get the number of successful writes
        Map<KVPair, WriteResult> resultsMap=ctx.currentResults();
        int successCount = 0;
        for(Map.Entry<KVPair,WriteResult> entry:resultsMap.entrySet()){
            WriteResult wr = entry.getValue();
            if(wr.getCode()==Code.SUCCESS){
                successCount++;
            }
        }

        statisticsWatcher.rowsWritten(successCount);
    }
}
