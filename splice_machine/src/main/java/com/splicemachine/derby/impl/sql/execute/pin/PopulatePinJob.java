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

package com.splicemachine.derby.impl.sql.execute.pin;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import java.util.concurrent.Callable;

/**
 * Class to populate a table into memory.  This still has a bit
 * of work to do before it will be enabled in the parser.
 *
 */
public class PopulatePinJob implements Callable<Void> {
    private final DistributedPopulatePinJob request;
    private final OlapStatus jobStatus;
    private long conglomID;

    public PopulatePinJob(DistributedPopulatePinJob request, OlapStatus jobStatus, long conglomID) {
        this.request = request;
        this.jobStatus = jobStatus;
        this.conglomID = conglomID;
    }

    @Override
    public Void call() throws Exception {
        if (!jobStatus.markRunning()) {
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.jobGroup, "");
        String scope = request.scope;
        DataSet<LocatedRow> dataSet = request.scanSetBuilder.buildDataSet(request.prefix);
        dataSet.pin(request.scanSetBuilder.getTemplate(),conglomID);
        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }
}
