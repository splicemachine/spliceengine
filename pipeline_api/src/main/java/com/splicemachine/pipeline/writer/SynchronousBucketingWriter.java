/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.writer;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.client.ActionStatusReporter;
import com.splicemachine.pipeline.client.BulkWriteAction;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.config.CountingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.writerstatus.ActionStatusMonitor;

import javax.annotation.Nonnull;
import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class SynchronousBucketingWriter implements Writer{
    private final ActionStatusReporter statusMonitor;
    private ActionStatusMonitor monitor;
    private final BulkWriterFactory writerFactory;
    private final PipelineExceptionFactory exceptionFactory;
    private final PartitionFactory partitionFactory;
    private final Clock clock;

    public SynchronousBucketingWriter(BulkWriterFactory writerFactory,
                                      PipelineExceptionFactory exceptionFactory,
                                      PartitionFactory partitionFactory,
                                      Clock clock){
        this.writerFactory=writerFactory;
        this.exceptionFactory=exceptionFactory;
        this.partitionFactory=partitionFactory;
        this.statusMonitor=new ActionStatusReporter();
        this.monitor=new ActionStatusMonitor(statusMonitor);
        this.clock = clock;

    }

    @Override
    public Future<WriteStats> write(byte[] tableName,
                                    BulkWrites bulkWrites,
                                    WriteConfiguration writeConfiguration) throws ExecutionException{
        WriteConfiguration countingWriteConfiguration=new CountingWriteConfiguration(writeConfiguration,statusMonitor,exceptionFactory);
        assert bulkWrites!=null:"bulk writes passed in are null";
        BulkWriteAction action=new BulkWriteAction(tableName,
                bulkWrites,
                countingWriteConfiguration,
                statusMonitor,
                writerFactory,
                exceptionFactory,
                partitionFactory,
                clock);
        statusMonitor.totalFlushesSubmitted.incrementAndGet();
        Exception e=null;
        WriteStats stats=null;
        try{
            stats=action.call();
        }catch(Exception error){
            e=error;
        }
        return new FinishedFuture(e,stats);
    }

    @Override
    public void stopWrites(){
        //no-op
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException{
        ObjectName monitorName=new ObjectName("com.splicemachine.writer.synchronous:type=WriterStatus");
        mbs.registerMBean(monitor,monitorName);
    }

    private static class FinishedFuture implements Future<WriteStats>{
        private final WriteStats stats;
        private Exception e;

        public FinishedFuture(Exception e,WriteStats stats){
            this.e=e;
            this.stats=stats;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning){
            return false;
        }

        @Override
        public boolean isCancelled(){
            return false;
        }

        @Override
        public boolean isDone(){
            return true;
        }

        @Override
        public WriteStats get() throws InterruptedException, ExecutionException{
            if(e instanceof ExecutionException) throw (ExecutionException)e;
            else if(e instanceof InterruptedException) throw (InterruptedException)e;
            else if(e!=null) throw new ExecutionException(e);
            return stats;
        }

        @Override
        public WriteStats get(long timeout,@Nonnull TimeUnit unit) throws InterruptedException,
                ExecutionException, TimeoutException{
            return get();
        }
    }
}
