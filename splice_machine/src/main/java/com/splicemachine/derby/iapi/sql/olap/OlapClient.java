package com.splicemachine.derby.iapi.sql.olap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by dgomezferro on 3/16/16.
 */
public interface OlapClient {
    /**
     *
     * Synchronously execute the expected job.
     *
     * <em>Handling Interruption</em>
     *
     * If the calling thread is interrupted during the execution of this job, the thread's interrupt
     * status will be set, and we will throw an IOException.
     *
     * @param jobRequest the job to run. Cannot be null
     * @param <R> the Type of OlapResult expected back from the server
     * @return the result of this job.
     * @throws IOException if something goes wrong communicating with the OlapServer
     * @throws TimeoutException if the operations timed out and needs to be aborted
     */
    <R extends OlapResult> R execute(@Nonnull DistributedJob jobRequest) throws IOException,TimeoutException;

    void shutdown();
}
