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
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.IntArrays;
import org.apache.spark.sql.types.StructType;

import java.nio.file.Path;
import java.sql.Struct;
import java.util.concurrent.Callable;

/**
 * Created by jfilali on 11/14/16.
 * Use to get the schema of the external file.
 */
public class GetSchemaExternalJob implements Callable<Void> {
    private final DistributedGetSchemaExternalJob request;
    private final OlapStatus jobStatus;


    public GetSchemaExternalJob(DistributedGetSchemaExternalJob request, OlapStatus jobStatus) {
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

        StructType externalSchema = dsp.getExternalFileSchema(request.getStoredAs(), request.getLocation());
        jobStatus.markCompleted(new GetSchemaExternalResult(externalSchema));
        return null;
    }

    public DistributedGetSchemaExternalJob getRequest() {
        return request;
    }

    public OlapStatus getJobStatus() {
        return jobStatus;
    }
}