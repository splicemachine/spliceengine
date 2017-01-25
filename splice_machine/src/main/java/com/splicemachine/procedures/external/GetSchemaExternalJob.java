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