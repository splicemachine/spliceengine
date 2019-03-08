/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.output.BulkLoadIndexDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.si.api.txn.TxnView;

import java.util.concurrent.Callable;

/**
 * Created by jyuan on 10/4/17.
 */
public class BulkLoadIndexJobImpl implements Callable<Void> {

    private final BulkLoadIndexJob request;
    private final OlapStatus jobStatus;

    public BulkLoadIndexJobImpl(BulkLoadIndexJob request, OlapStatus jobStatus) {
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
        bulkLoadIndex(request.ah, request.tentativeIndex,request.scanSetBuilder,request.prefix,
                request.childTxn);
        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }

    private DataSet<ExecRow> bulkLoadIndex(ActivationHolder ah,
                                           DDLMessage.TentativeIndex tentativeIndex,
                                           ScanSetBuilder<ExecRow> scanSetBuilder,
                                           String prefix,
                                           TxnView childTxn)  throws StandardException {

        DataSet<ExecRow> dataSet = scanSetBuilder.buildDataSet(prefix);
        DataSetProcessor dsp =EngineDriver.driver().processorFactory().distributedProcessor();
        scanSetBuilder.operationContext(dsp.createOperationContext(ah.getActivation()));
        OperationContext operationContext = scanSetBuilder.getOperationContext();

        BulkLoadIndexDataSetWriterBuilder writerBuilder = dataSet
                .bulkLoadIndex(operationContext)
                .bulkLoadDirectory(request.hfilePath)
                .sampling(request.sampling)
                .bulkLoadDirectory(request.hfilePath)
                .tentativeIndex(request.tentativeIndex)
                .indexName(request.indexName)
                .tableVersion(request.tableVersion);

        DataSetWriter writer = writerBuilder
                .destConglomerate(tentativeIndex.getIndex().getConglomerate())
                .operationContext(operationContext)
                .txn(childTxn)
                .build();

        return writer.write();
    }
}
