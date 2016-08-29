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

package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.pipeline.Exceptions;
import org.apache.log4j.Logger;
import org.spark_project.guava.util.concurrent.ListenableFuture;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class TimedOlapClient implements OlapClient{

    protected static final Logger LOG = Logger.getLogger(TimedOlapClient.class);

    private final int timeoutMillis;
    private final JobExecutor networkLayer;

    public TimedOlapClient(JobExecutor networkLayer,int timeoutMillis){
        this.timeoutMillis=timeoutMillis;
        this.networkLayer = networkLayer;
    }

    @Override
    public <R extends OlapResult> R execute(@Nonnull DistributedJob jobRequest) throws IOException,TimeoutException{
        jobRequest.markSubmitted();
        //submit the jobRequest to the server
        try{
            Future<OlapResult> submit=networkLayer.submit(jobRequest);
            //noinspection unchecked
            return (R)submit.get(timeoutMillis,TimeUnit.MILLISECONDS);
        }catch(InterruptedException e){
            //we were interrupted processing, so we're shutting down. Nothing to be done, just die gracefully
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }catch(ExecutionException e){
            throw Exceptions.rawIOException(e.getCause());
        }
    }

    @Override
    public <R extends OlapResult> ListenableFuture<R> submit(@Nonnull DistributedJob jobRequest) throws IOException {
        jobRequest.markSubmitted();
        return (ListenableFuture<R>) networkLayer.submit(jobRequest);
    }

    @Override
    public void shutdown(){
        networkLayer.shutdown();
    }

}
