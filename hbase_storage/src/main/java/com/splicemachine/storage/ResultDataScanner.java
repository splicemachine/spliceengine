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
public class ResultDataScanner implements RecordScanner {
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
