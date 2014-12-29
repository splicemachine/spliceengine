package com.splicemachine.derby.hbase;

import com.google.common.collect.Maps;
import com.splicemachine.pipeline.api.WriteBufferFactory;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteContextFactory;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writecontextfactory.LocalWriteContextFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class PipelineContextFactories {
    private static final DiscardingWriteContext<TransactionalRegion> UNMANAGED_CTX_FACTORY
            = new DiscardingWriteContext<>(-1l, LocalWriteContextFactory.unmanagedContextFactory());
    private static final ConcurrentMap<Long,DiscardingWriteContext<TransactionalRegion>> ctxMap
            = new ConcurrentHashMap<>();
    static{
        UNMANAGED_CTX_FACTORY.register(); //always hold on to the unmanaged factory so that it never closes
        ctxMap.put(-1l,UNMANAGED_CTX_FACTORY);
    }

    public static WriteContextFactory<TransactionalRegion> getWriteContext(long conglomerateId){
        DiscardingWriteContext<TransactionalRegion> ctxFactory = ctxMap.get(conglomerateId);
        if(ctxFactory==null){
            DiscardingWriteContext<TransactionalRegion> newFactory = new DiscardingWriteContext<>(conglomerateId, new LocalWriteContextFactory(conglomerateId));
            DiscardingWriteContext<TransactionalRegion> oldFactory = ctxMap.putIfAbsent(conglomerateId, newFactory);
            if(oldFactory!=null)
                ctxFactory = oldFactory;
            else
                ctxFactory = newFactory;
        }
        ctxFactory.register();
        return ctxFactory;
    }

    private static class DiscardingWriteContext<T> implements WriteContextFactory<T>{
        private final long conglomId;
        private final WriteContextFactory<T> delegate;
        private AtomicInteger refCount = new AtomicInteger(0);
        private volatile boolean closed = false;

        public DiscardingWriteContext(long conglomId, WriteContextFactory<T> delegate) {
            this.conglomId = conglomId;
            this.delegate = delegate;
        }

        @Override
        public WriteContext create(WriteBufferFactory indexSharedCallBuffer,
                                   TxnView txn,
                                   T key,
                                   RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
            return delegate.create(indexSharedCallBuffer, txn, key, env);
        }

        @Override
        public WriteContext create(WriteBufferFactory indexSharedCallBuffer,
                                   TxnView txn,
                                   T key,
                                   int expectedWrites,
                                   RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
            return delegate.create(indexSharedCallBuffer, txn, key, expectedWrites, env);
        }

        @Override
        public WriteContext createPassThrough(WriteBufferFactory indexSharedCallBuffer,
                                              TxnView txn,
                                              T key,
                                              int expectedWrites,
                                              RegionCoprocessorEnvironment env) throws IOException, InterruptedException {
            return delegate.createPassThrough(indexSharedCallBuffer, txn, key, expectedWrites, env);
        }

        @Override
        public void dropIndex(long indexConglomId, TxnView txn) {
            delegate.dropIndex(indexConglomId, txn);
        }

        @Override
        public void addIndex(DDLChange ddlChange, int[] columnOrdring, int[] typeIds) {
            delegate.addIndex(ddlChange, columnOrdring, typeIds);
        }

        @Override
        public void addDDLChange(DDLChange ddlChange) {
            delegate.addDDLChange(ddlChange);
        }

        @Override
        public void close() {
            int remaining = refCount.decrementAndGet();
            if(remaining<=0){
                closed=true;
                delegate.close();
                ctxMap.remove(conglomId,this); //remove us from the map
            }
        }

        @Override
        public void prepare() {
            delegate.prepare();
        }

        @Override
        public boolean hasDependentWrite() {
            return delegate.hasDependentWrite();
        }

        public void register(){
            assert !closed: "Cannot register with a closed Factory!";
            refCount.incrementAndGet();
        }

    }
}
