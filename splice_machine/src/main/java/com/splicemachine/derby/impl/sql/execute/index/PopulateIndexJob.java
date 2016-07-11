package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.EngineDriver;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.output.DataSetWriter;
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

        DDLMessage.TentativeIndex tentativeIndex = request.tentativeIndex;
        String scope = request.scope;
        DataSet<LocatedRow> dataSet = request.scanSetBuilder.buildDataSet(request.prefix);
        PairDataSet dsToWrite = dataSet
                .map(new IndexTransformFunction(tentativeIndex,request.indexFormatIds), null, false, true, scope + ": Prepare Index")
                .index(new KVPairFunction(), false, true, scope + ": Populate Index");
        DataSetWriter writer = dsToWrite.directWriteData()
                .destConglomerate(tentativeIndex.getIndex().getConglomerate())
                .txn(request.childTxn)
                .build();
        @SuppressWarnings("unused") DataSet<LocatedRow> result = writer.write();
        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }
}
