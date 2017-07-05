/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.pin;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
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
        DataSet<ExecRow> dataSet = request.scanSetBuilder.buildDataSet(request.prefix);
        dataSet.pin(request.scanSetBuilder.getTemplate(),conglomID);
        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }
}
