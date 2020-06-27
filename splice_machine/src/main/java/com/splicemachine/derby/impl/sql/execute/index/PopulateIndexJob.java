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
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.function.SplicePredicateFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.txn.TxnView;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class PopulateIndexJob implements Callable<Void> {
    private final DistributedPopulateIndexJob request;
    private final OlapStatus jobStatus;

    public PopulateIndexJob(DistributedPopulateIndexJob request, OlapStatus jobStatus) {
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
        populateIndex(request.tentativeIndex,request.scanSetBuilder,request.prefix,request.indexFormatIds,request.scope,request.childTxn);
        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }

    public static DataSet<ExecRow> populateIndex(DDLMessage.TentativeIndex tentativeIndex,
                                                 ScanSetBuilder<ExecRow> scanSetBuilder,
                                                 String prefix,
                                                 int[] indexFormatIds,
                                                 String scope,
                                                 TxnView childTxn
                                                    ) throws StandardException
    {

        DataSet<ExecRow> dataSet = scanSetBuilder.buildDataSet(prefix);
        OperationContext operationContext = scanSetBuilder.getOperationContext();
        DataSet<KVPair> keyValueSet = dataSet
                .map(new IndexTransformFunction(tentativeIndex), null, false, true, scope + ": Prepare Index");
        if (tentativeIndex.getIndex().getExcludeDefaults() || tentativeIndex.getIndex().getExcludeNulls()) {
            keyValueSet = keyValueSet.filter(new NullFilter<>());
        }
        PairDataSet dsToWrite = keyValueSet.index(new KVPairFunction(), false, true, scope + ": Populate Index");
        DataSetWriter writer = dsToWrite.directWriteData()
                .operationContext(operationContext)
                .destConglomerate(tentativeIndex.getIndex().getConglomerate())
                .txn(childTxn)
                .build();
        return writer.write();
    }

    public static class NullFilter <Op extends SpliceOperation> extends SplicePredicateFunction<Op,KVPair> {

        public NullFilter(){}

        @Override
        public boolean apply(@Nullable KVPair pair) {
            return pair != null;
        }
    }
}
