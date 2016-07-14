/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage.util;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

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

