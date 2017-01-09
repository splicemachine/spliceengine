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

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 1/29/16
 */
public abstract class ReferenceCountingFactoryDriver implements ContextFactoryDriver{
    private final ConcurrentMap<Long,CountingFactoryLoader> loaderMap = new ConcurrentHashMap<>();

    @Override
    public ContextFactoryLoader getLoader(long conglomerateId){
        synchronized(this){
            CountingFactoryLoader countingFactoryLoader=loaderMap.get(conglomerateId);
            if(countingFactoryLoader==null){
                countingFactoryLoader=new CountingFactoryLoader(conglomerateId,newDelegate(conglomerateId));
                CountingFactoryLoader oldLoader=loaderMap.putIfAbsent(conglomerateId,countingFactoryLoader);
                if(oldLoader!=null)
                    countingFactoryLoader=oldLoader;
            }
            countingFactoryLoader.refCount.incrementAndGet();
            return countingFactoryLoader;
        }
    }

    protected abstract ContextFactoryLoader newDelegate(long conglomerateId);

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private class CountingFactoryLoader implements ContextFactoryLoader{
        private final ContextFactoryLoader delegate;
        private final long conglomId;
        private final AtomicInteger refCount = new AtomicInteger(0);
        private volatile boolean loaded;

        public CountingFactoryLoader(long conglomId,ContextFactoryLoader delegate){
            this.delegate=delegate;
            this.conglomId = conglomId;
        }

        @Override
        public void close(){
           int rCount= refCount.decrementAndGet();
           if(rCount==0){
               synchronized(ReferenceCountingFactoryDriver.this){
                   if(refCount.get()<=0)
                       loaderMap.remove(conglomId);
               }
           }
        }

        @Override
        public void load(Txn txn) throws IOException, InterruptedException{
            if(loaded) return; //no need to load twice
            delegate.load(txn);
            loaded=true;
        }

        @Override
        public WriteFactoryGroup getForeignKeyFactories(){
            return delegate.getForeignKeyFactories();
        }

        @Override
        public WriteFactoryGroup getIndexFactories(){
            return delegate.getIndexFactories();
        }

        @Override
        public WriteFactoryGroup getDDLFactories(){
            return delegate.getDDLFactories();
        }

        @Override
        public Set<ConstraintFactory> getConstraintFactories(){
            return delegate.getConstraintFactories();
        }

        @Override
        public void ddlChange(DDLMessage.DDLChange ddlChange){
            if(!loaded) return; //ignore changes that occur before we have a chance to load them
            delegate.ddlChange(ddlChange);
        }
    }
}
