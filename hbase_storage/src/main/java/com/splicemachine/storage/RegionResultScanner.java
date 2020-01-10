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
