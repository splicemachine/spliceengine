package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.TxnView;

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
        public void load(TxnView txn) throws IOException, InterruptedException{
            delegate.load(txn);
        }

        @Override
        public void unload(){
            delegate.unload();
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
            delegate.ddlChange(ddlChange);
        }
    }
}
