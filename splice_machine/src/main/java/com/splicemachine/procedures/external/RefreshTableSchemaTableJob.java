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

package com.splicemachine.procedures.external;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;

import java.util.concurrent.Callable;

/**
 * Created by jfilali on 11/14/16.
 * Job used to refresh the external table
 */
public class RefreshTableSchemaTableJob implements Callable<Void> {
    private final DistributedRefreshExternalTableSchemaJob request;
    private final OlapStatus jobStatus;


    public RefreshTableSchemaTableJob(DistributedRefreshExternalTableSchemaJob request, OlapStatus jobStatus) {
        this.request = request;
        this.jobStatus = jobStatus;

    }

    @Override
    public Void call() throws Exception {

        if (!jobStatus.markRunning()) {
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.getJobGroup(), "");
        dsp.refreshTable(request.getLocation());

        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }

}
