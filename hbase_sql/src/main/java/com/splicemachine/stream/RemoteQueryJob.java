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

package com.splicemachine.stream;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.ActivationHolder;

import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 5/20/16.
 */
public class RemoteQueryJob extends DistributedJob {
    final UUID uuid;
    int rootResultSetNumber;
    ActivationHolder ah;
    String host;
    int port;
    String userId;
    String sql;
    int streamingBatches;
    int streamingBatchSize;


    public RemoteQueryJob(ActivationHolder ah, int rootResultSetNumber, UUID uuid, String host, int port,
                          String userId, String sql,
                          int streamingBatches, int streamingBatchSize) {
        this.ah = ah;
        this.rootResultSetNumber = rootResultSetNumber;
        this.uuid = uuid;
        this.host = host;
        this.port = port;
        this.userId = userId;
        this.sql = sql;
        this.streamingBatches = streamingBatches;
        this.streamingBatchSize = streamingBatchSize;
    }

    @Override
    public Callable<Void> toCallable(OlapStatus jobStatus, Clock clock, long clientTimeoutCheckIntervalMs) {
        return new QueryJob(this, jobStatus);
    }

    @Override
    public String getName() {
        return "query-"+uuid;
    }
}
