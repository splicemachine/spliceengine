/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class MeasuredResultScanner implements ResultScanner{
    private final ResultScanner resultScanner;
    private final Timer timer;
    private final Counter outputBytesCounter;

    public MeasuredResultScanner(ResultScanner resultScanner,MetricFactory metricFactory){
        this.resultScanner=resultScanner;
        this.timer = metricFactory.newTimer();
        this.outputBytesCounter = metricFactory.newCounter();
    }

    @Override
    public Result next() throws IOException{
        timer.startTiming();
        Result r = resultScanner.next();
        timer.tick(r==null?0l:1l);
        if(outputBytesCounter.isActive())
            countOutputBytes(r);
        return r;
    }

    @Override
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public Result[] next(int nbRows) throws IOException{
        timer.startTiming();
        Result[] next=resultScanner.next(nbRows);
        timer.tick(next.length);
        if(outputBytesCounter.isActive()){
            for(int i=0;i<next.length;i++){
                countOutputBytes(next[i]);
            }
        }
        return next;
    }

    @Override
    public void close(){
        resultScanner.close();
    }

    public boolean renewLease() {
        return false;
    }

    public ScanMetrics getScanMetrics() {
        return resultScanner.getScanMetrics();
    }

    @Override
    public Iterator<Result> iterator(){
        return new PeekIterator();
    }

    public TimeView getTime(){
        return timer.getTime();
    }

    public long getBytesOutput(){
        return outputBytesCounter.getTotal();
    }

    public long getRowsFiltered(){
        return 0;
    }

    public long getRowsVisited(){
        return timer.getNumEvents();
    }

    /* ****************************************************************************************************************/
    private void countOutputBytes(Result r){
        if(r==null || r.size()<=0) return;
        //TODO -sf- count the cell bytes
    }

    private class PeekIterator implements Iterator<Result>{
        private Result currResult;
        @Override
        public boolean hasNext(){
            try{
                currResult=MeasuredResultScanner.this.next();
            }catch(IOException e){
                throw new RuntimeException(e);
            }
            return currResult!=null;
        }

        @Override
        public Result next(){
            Result r = currResult;
            if(r==null) throw new NoSuchElementException();
            currResult = null;
            return r;
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException("Remove not supported!");
        }
    }
}

