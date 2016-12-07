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