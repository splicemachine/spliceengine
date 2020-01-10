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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.utils.IntArrays;
import org.apache.spark.sql.types.StructField;


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

        ColumnInfo[] columnInfo = request.getColumnInfo();
        ExecRow execRow = new ValueRow(columnInfo.length);
        DataValueDescriptor[] dvds = execRow.getRowArray();
        for (int i = 0; i < columnInfo.length; ++i) {
            dvds[i] = columnInfo[i].dataType.getNull();
        }

        int[] execRowTypeFormatIds= WriteReadUtils.getExecRowTypeFormatIds(execRow);

        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        dsp.setSchedulerPool("admin");
        dsp.setJobGroup(request.getJobGroup(), "");

        //Generate StructField Array
        StructField[] fields = new StructField[columnInfo.length];
        for (int i = 0; i < columnInfo.length;i++) {
            fields[i] = dvds[i].getStructField(columnInfo[i].name);
        }
        dsp.createEmptyExternalFile(fields, IntArrays.count(execRowTypeFormatIds.length), request.getPartitionBy(),  request.getStoredAs(), request.getLocation(),request.getCompression());


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
