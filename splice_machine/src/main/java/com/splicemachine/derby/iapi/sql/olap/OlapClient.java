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
     * Submits a job for asynchronous execution to the default queue.
     *
     * @param jobRequest the job to run. Cannot be null
     * @param <R> the Type of OlapResult expected back from the server, which will be wrapped in a Future
     * @return a cancellable Future that, when completed, contains the result of this job.
     * @throws IOException if something goes wrong communicating with the OlapServer
     */
    <R extends OlapResult> ListenableFuture<R> submit(@Nonnull DistributedJob jobRequest) throws IOException;


    /**
     *
     * Submits a job for asynchronous execution to the specified queue.
     *
     * @param jobRequest the job to run. Cannot be null
     * @param queue name of the queue. Cannot be null
     * @param <R> the Type of OlapResult expected back from the server, which will be wrapped in a Future
     * @return a cancellable Future that, when completed, contains the result of this job.
     * @throws IOException if something goes wrong communicating with the OlapServer
     */
    <R extends OlapResult> ListenableFuture<R> submit(@Nonnull DistributedJob jobRequest, String queue) throws IOException;

    void shutdown();
}
