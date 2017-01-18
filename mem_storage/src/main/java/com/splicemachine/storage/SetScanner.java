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

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class SetScanner implements RecordScanner {
    private final long sequenceCutoffPoint;
    private final long lowVersion;
    private final long highVersion;
    private final Partition partition;
    private final Counter rowCounter;
    private final Counter filterCounter;

    public SetScanner(long sequenceCutoffPoint,
                      long lowVersion,
                      long highVersion,
                      Partition partition,
                      MetricFactory metricFactory){
        this.sequenceCutoffPoint=sequenceCutoffPoint;
        this.lowVersion=lowVersion;
        this.highVersion=highVersion;
        this.partition = partition;
        this.rowCounter = metricFactory.newCounter();
        this.filterCounter = metricFactory.newCounter();
    }

    @Override
    public @Nonnull Record next() throws IOException{
        throw new UnsupportedOperationException("not implemented");
    }


    @Override
    public TimeView getReadTime(){
        return Metrics.noOpTimeView();
    }

    @Override
    public long getBytesOutput(){
        return 0;
    }

    @Override
    public long getRowsFiltered(){
        return filterCounter.getTotal();
    }

    @Override
    public long getRowsVisited(){
        return rowCounter.getTotal();
    }

    @Override
    public void close() throws IOException{

    }

}
