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

package com.splicemachine.procedures.external;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.IntArrays;


import java.nio.file.Path;
import java.util.concurrent.Callable;

/**
 * Created by jfilali on 11/14/16.
 * Job used to create a external empty file if there is not file
 * associated to the location provided in the query.
 * This is useful to avoid issues with commands that will try to access
 * a file that doesn't exists.
 */
public class CreateExternalTableJob implements Callable<Void> {
    private final DistributedCreateExternalTableJob request;
    private final OlapStatus jobStatus;


    public CreateExternalTableJob(DistributedCreateExternalTableJob request, OlapStatus jobStatus) {
        this.request = request;
        this.jobStatus = jobStatus;

    }

    @Override
    public Void call() throws Exception {

        if (!jobStatus.markRunning()) {
            //the client has already cancelled us or has died before we could get started, so stop now
            return null;
        }

        ExecRow execRow = request.getExecRow();
        int[] execRowTypeFormatIds= WriteReadUtils.getExecRowTypeFormatIds(execRow);

        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.getJobGroup(), "");

        // look at the file, if it doesn't exist create it.

        if(!SIDriver.driver().fileSystem().getInfo(request.getLocation()).exists()){
            Path pathToParent = SIDriver.driver().fileSystem().getPath(request.getLocation()).getParent();
            ImportUtils.validateWritable(pathToParent.toString(),false);
            dsp.createEmptyExternalFile(execRow, IntArrays.count(execRowTypeFormatIds.length), request.getPartitionBy(),  request.getStoredAs(), request.getLocation(),request.getCompression());
        }

        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }

    public DistributedCreateExternalTableJob getRequest() {
        return request;
    }

    public OlapStatus getJobStatus() {
        return jobStatus;
    }
}