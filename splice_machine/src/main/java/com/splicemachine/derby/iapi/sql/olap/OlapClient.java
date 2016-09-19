/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.iapi.sql.olap;

import org.spark_project.guava.util.concurrent.ListenableFuture;

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

    /**
     *
     * Submits a job for asynchronous execution.
     *
     * @param jobRequest the job to run. Cannot be null
     * @param <R> the Type of OlapResult expected back from the server, which will be wrapped in a Future
     * @return a cancellable Future that, when completed, contains the result of this job.
     * @throws IOException if something goes wrong communicating with the OlapServer
     */
    <R extends OlapResult> ListenableFuture<R> submit(@Nonnull DistributedJob jobRequest) throws IOException;

    void shutdown();
}
