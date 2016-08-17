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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class MappedJobRegistry implements OlapJobRegistry{
    private static final Logger LOG =Logger.getLogger(MappedJobRegistry.class);
    private final ConcurrentMap<String/*jobName*/,OlapJobStatus/*jobStatus*/> registry;
    private final ScheduledExecutorService registryCleaner;
    private final long tickTime;
    private final int numTicks;

    public MappedJobRegistry(long tickTime,int numTicks,TimeUnit unit){
        this.tickTime=unit.toMillis(tickTime);
        this.numTicks = numTicks;
        this.registry = new ConcurrentHashMap<>();
        this.registryCleaner =Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("jobRegistryCleaner").build());
        this.registryCleaner.scheduleWithFixedDelay(new Cleaner(),1l,10l,unit);
    }

    @Override
    public OlapJobStatus register(String uniqueJobName){
        if(LOG.isTraceEnabled())
            LOG.trace("registering job with name "+uniqueJobName);
        /*
         * This is probably over safe, since 99% of the time we will be submitting for the first time,
         * but this way it guarantees idempotency of registration, so we can safely retry
         * client calls that fail
         */
        OlapJobStatus status = registry.get(uniqueJobName);
        if(status!=null) return status;
        status = new OlapJobStatus(tickTime,numTicks);
        OlapJobStatus old = registry.putIfAbsent(uniqueJobName,status);
        if(old!=null)
            status = old;
        return status;
    }

    @Override
    public OlapJobStatus getStatus(String jobId){
        if(LOG.isTraceEnabled())
            LOG.trace("getting Status for job "+ jobId);
        OlapJobStatus olapJobStatus=registry.get(jobId);
        if(LOG.isTraceEnabled() && olapJobStatus!=null)
            LOG.trace("Status for job "+jobId+"="+olapJobStatus.currentState());
        return olapJobStatus;
    }

    @Override
    public void clear(String jobId){
        if(LOG.isTraceEnabled())
            LOG.trace("Clearing job "+ jobId);

        OlapJobStatus jobStatus = registry.remove(jobId);
        if (jobStatus != null) {
            jobStatus.cancel();
        }
    }

    @Override
    public long tickTime(){
        return tickTime;
    }

    private class Cleaner implements Runnable{
        @Override
        public void run(){
            Iterator<Map.Entry<String,OlapJobStatus>> regIterator = registry.entrySet().iterator();
            while(regIterator.hasNext()){
                Map.Entry<String,OlapJobStatus> entry = regIterator.next();
                if(!entry.getValue().isAvailable()){
                    if(LOG.isTraceEnabled())
                        LOG.trace("Job with id "+ entry.getKey()+" does not have an available client, removing");
                    regIterator.remove();
                    entry.getValue().cancel();
                }
            }
        }
    }
}
