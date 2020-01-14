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
 *
 */

package com.splicemachine.stream;

import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.si.impl.server.CompactionContext;
import org.apache.spark.util.LongAccumulator;

import java.io.Serializable;

public class SparkCompactionContext implements CompactionContext, Serializable {
    private final LongAccumulator rowsRead;
    private final LongAccumulator resolutionCached;
    private final LongAccumulator commitRead;
    private final LongAccumulator resolutionScheduled;
    private final LongAccumulator dataRead;
    private final LongAccumulator timeouts;
    private final LongAccumulator unresolved;
    private final LongAccumulator resolutionRejected;
    private final LongAccumulator rpc;
    private final LongAccumulator timeBlocked;

    public SparkCompactionContext() {
        this.rowsRead= SpliceSpark.getContext().sc().longAccumulator("rows read");

        this.dataRead = SpliceSpark.getContext().sc().longAccumulator("data entries read");
        this.commitRead = SpliceSpark.getContext().sc().longAccumulator("commit entries read");

        this.timeouts = SpliceSpark.getContext().sc().longAccumulator("resolutions timed out");
        this.unresolved = SpliceSpark.getContext().sc().longAccumulator("unresolved transaction");

        this.resolutionCached = SpliceSpark.getContext().sc().longAccumulator("resolutions cached");
        this.resolutionScheduled = SpliceSpark.getContext().sc().longAccumulator("resolutions scheduled");
        this.resolutionRejected = SpliceSpark.getContext().sc().longAccumulator("resolutions rejected");

        this.rpc = SpliceSpark.getContext().sc().longAccumulator("rpcs");
        this.timeBlocked = SpliceSpark.getContext().sc().longAccumulator("time blocked");
    }

    @Override
    public void readData(){
        dataRead.add(1l);
    }

    @Override
    public void readCommit() {
        commitRead.add(1l);
    }

    @Override
    public void rowRead() {
        rowsRead.add(1l);
    }

    @Override
    public void recordTimeout() {
        timeouts.add(1l);
    }

    @Override
    public void recordUnresolvedTransaction() {
        unresolved.add(1l);
    }

    @Override
    public void recordResolutionRejected() {
        resolutionRejected.add(1l);
    }

    @Override
    public void recordRPC() {
        rpc.add(1l);
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void timeBlocked(long duration) {
        timeBlocked.add(duration);
    }

    @Override
    public void recordResolutionScheduled() {
        resolutionScheduled.add(1l);
    }

    @Override
    public void recordResolutionCached() {
        resolutionCached.add(1l);
    }
}
