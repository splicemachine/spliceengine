/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
