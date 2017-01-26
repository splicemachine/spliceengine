/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage.util;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;


/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class MeasuredListScanner implements AutoCloseable{
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

    public boolean next(List<Cell> list) throws IOException{
        timer.startTiming();
        boolean b=delegate.nextRaw(list);
        timer.tick(list.size()>0?1l:0l);

        if(outputBytesCounter.isActive())
            countOutput(list);
        return b;
    }

    public boolean next(List<Cell> list,int limit) throws IOException{
        //TODO -sf- do not ignore the limit
        return next(list);
    }

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
