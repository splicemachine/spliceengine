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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.si.impl.driver.SIDriver;
import org.spark_project.guava.util.concurrent.Futures;
import org.spark_project.guava.util.concurrent.ListenableFuture;
import org.spark_project.guava.util.concurrent.SettableFuture;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeoutException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class LocalOlapClient implements OlapClient{
    private static LocalOlapClient ourInstance=new LocalOlapClient();

    public static LocalOlapClient getInstance(){
        return ourInstance;
    }

    private LocalOlapClient(){ }


    @Override
    public <R extends OlapResult> R execute(@Nonnull DistributedJob jobRequest) throws IOException, TimeoutException{
        jobRequest.markSubmitted();
        Status status=new Status();
        Callable<Void> callable=jobRequest.toCallable(status,SIDriver.driver().getClock(),Long.MAX_VALUE);
        try {
            callable.call();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return (R)status.getResult();
    }

    @Override
    public <R extends OlapResult> ListenableFuture<R> submit(@Nonnull final DistributedJob jobRequest) throws IOException {
        try {
            SettableFuture result = SettableFuture.create();
            result.set(execute(jobRequest));
            return result;
        } catch (TimeoutException e) {
            throw new IOException(e);
        }
    }

    @Override public void shutdown(){ }

    /* ****************************************************************************************************************/
    /*private helper stuff*/
    private static class Status implements OlapStatus{
        private OlapResult result;

        @Override
        public State checkState(){
            return result==null? State.RUNNING:State.COMPLETE;
        }

        @Override
        public OlapResult getResult(){
            return result;
        }

        @Override
        public void cancel(){

        }

        @Override
        public boolean isAvailable(){
            return true;
        }

        @Override
        public boolean markSubmitted(){
            return true;
        }

        @Override
        public void markCompleted(OlapResult result){
            this.result = result;
        }

        @Override
        public boolean markRunning(){
            return true;
        }

        @Override
        public boolean isRunning(){
            return result==null;
        }
    }
}
