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

package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.*;

import java.util.concurrent.Callable;

/**
 * Created by jyuan on 10/9/18.
 */
public class SamplingJobImpl implements Callable<Void> {

    private final SamplingJob request;
    private final OlapStatus jobStatus;

    public SamplingJobImpl(SamplingJob request, OlapStatus jobStatus) {
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
        dsp.setJobGroup(request.jobGroup, "");
        byte[][] splitKeys = calculateSplitKey(request.ah, request.tentativeIndex,request.scanSetBuilder,request.prefix);

        jobStatus.markCompleted(new SamplingResult(splitKeys));
        return null;
    }

    private byte[][] calculateSplitKey(ActivationHolder ah,
                                               DDLMessage.TentativeIndex tentativeIndex,
                                               ScanSetBuilder<ExecRow> scanSetBuilder,
                                               String prefix)  throws StandardException {
        DataSet<ExecRow> dataSet = scanSetBuilder.buildDataSet(prefix);
        DataSetProcessor dsp =EngineDriver.driver().processorFactory().distributedProcessor();
        scanSetBuilder.operationContext(dsp.createOperationContext(ah.getActivation()));
        OperationContext operationContext = scanSetBuilder.getOperationContext();

        TableSampler tableSampler = dataSet
                .sample(operationContext)
                .tentativeIndex(request.tentativeIndex)
                .indexName(request.indexName)
                .sampleFraction(request.sampleFraction)
                .build();

        return tableSampler.sample();
    }
}
