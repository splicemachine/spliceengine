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
