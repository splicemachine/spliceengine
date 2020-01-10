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

package com.splicemachine.derby.hbase;

import com.splicemachine.pipeline.api.PipelineMeter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class CountingPipelineMeter implements PipelineMeter{
    private final AtomicLong rejectedCount = new AtomicLong(0l);

    private final AtomicLong successCounter = new AtomicLong(0l);
    private final AtomicLong failedCounter = new AtomicLong(0l);
    private final long startupTimestamp = System.nanoTime();

    @Override
    public void mark(int numSuccess,int numFailed){
        successCounter.addAndGet(numSuccess);
        failedCounter.addAndGet(numFailed);
    }

    @Override
    public void rejected(int numRows){
        rejectedCount.addAndGet(numRows);
    }

    @Override
    public double throughput(){
        return ((double)successCounter.get())/(System.nanoTime()-startupTimestamp);
    }

    @Override
    public double fifteenMThroughput(){
        return 0;
    }

    @Override
    public double fiveMThroughput(){
        return 0;
    }

    @Override
    public double oneMThroughput(){
        return 0;
    }

    @Override
    public long rejectedCount(){
        return rejectedCount.get();
    }
}
