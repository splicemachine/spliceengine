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

package com.splicemachine.derby.utils.stats;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;

import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class DistributedStatsCollection extends DistributedJob {
    String jobGroup;
    ScanSetBuilder scanSetBuilder;
    String scope;

    public DistributedStatsCollection() {}

    public DistributedStatsCollection(ScanSetBuilder scanSetBuilder, String scope, String jobGroup) {
        this.scanSetBuilder = scanSetBuilder;
        this.scope = scope;
        this.jobGroup = jobGroup;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new StatsCollectionJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return null;
    }
}
