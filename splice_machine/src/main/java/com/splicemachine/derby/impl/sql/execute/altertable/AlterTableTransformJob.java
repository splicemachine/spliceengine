package com.splicemachine.derby.impl.sql.execute.altertable;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.derby.iapi.sql.olap.SuccessfulOlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.function.RowTransformFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.storage.Record;

import java.util.concurrent.Callable;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class AlterTableTransformJob implements Callable<Void> {
    private final DistributedAlterTableTransformJob request;
    private final OlapStatus jobStatus;

    public AlterTableTransformJob(DistributedAlterTableTransformJob request, OlapStatus jobStatus) {
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
        dsp.setSchedulerPool(request.pool);
        dsp.setJobGroup(request.jobGroup, request.description);


        DataSet<Record> dataSet = request.scanSetBuilder.buildDataSet(this);

        // Write new conglomerate
        PairDataSet<LocatedRow,Record> ds = dataSet.map(new RowTransformFunction(request.ddlChange)).index(new KVPairFunction());
        //side effects are what matters here
        @SuppressWarnings("unused") DataSet<LocatedRow> result = ds.directWriteData()
                .txn(request.childTxn)
                .destConglomerate(request.destConglom)
                .skipIndex(true).build().write();
        jobStatus.markCompleted(new SuccessfulOlapResult());
        return null;
    }
}
