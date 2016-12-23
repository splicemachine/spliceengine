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
public class RegionResultScanner implements RecordScanner {
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
