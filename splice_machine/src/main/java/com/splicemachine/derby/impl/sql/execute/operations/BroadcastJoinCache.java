package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Supplier;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.stream.Stream;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a Cache of records for use in a Broadcast Join.
 *
 * @author Scott Fines
 *         Date: 10/27/15
 */
public class BroadcastJoinCache{
    private final Cache<Long,ReferenceCountingFactory> cache;
    private final JoinTableLoader tableLoader;

    interface JoinTableLoader{
        JoinTable.Factory load(Callable<Stream<ExecRow>> streamLoader,
                               int[] innerHashKeys,
                               int[] outerHashKeys,
                               ExecRow outerTemplateRow) throws ExecutionException;
    }

    public BroadcastJoinCache(){
       this(ByteBufferMapTableLoader.INSTANCE);
    }

    public BroadcastJoinCache(JoinTableLoader tableLoader){
        this.tableLoader = tableLoader;
        this.cache =CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterAccess(2,TimeUnit.SECONDS)
                .softValues()
                .build();
    }

    public JoinTable.Factory get(byte[] operationId,
                         Callable<Stream<ExecRow>> loader,
                         int[] rightHashKeys,
                         int[] leftHashKeys,
                         ExecRow leftTemplateRow) throws IOException, StandardException{
        try{
            Long k=Bytes.toLong(operationId);
            Loader callable=new Loader(k,tableLoader,rightHashKeys,leftHashKeys,leftTemplateRow,loader);
            ReferenceCountingFactory joinTable=cache.get(k,callable);
            joinTable.refCount.incrementAndGet();

            return joinTable;
        }catch(ExecutionException e){
            Throwable c = e.getCause();
            if(c instanceof StandardException)
                throw (StandardException)c;
            else if(c instanceof IOException)
                throw (IOException)c;
            else throw Exceptions.getIOException(c);
        }
    }

    private class Loader implements Callable<ReferenceCountingFactory>{
        private final JoinTableLoader loader;
        private final int[] innerHashKeys;
        private final int[] outerHashKeys;
        private final ExecRow outerTemplateRow;
        private final Callable<Stream<ExecRow>> streamLoader;

        private final Long operationId;

        public Loader(Long operationId,
                      JoinTableLoader loader,
                      int[] innerHashKeys,
                      int[] outerHashKeys,
                      ExecRow outerTemplateRow,
                      Callable<Stream<ExecRow>> streamLoader){
            this.loader=loader;
            this.operationId=operationId;
            this.innerHashKeys=innerHashKeys;
            this.outerHashKeys=outerHashKeys;
            this.outerTemplateRow=outerTemplateRow;
            this.streamLoader=streamLoader;
        }

        @Override
        public ReferenceCountingFactory call() throws Exception{
            JoinTable.Factory load=loader.load(streamLoader,innerHashKeys,outerHashKeys,outerTemplateRow);
            return new ReferenceCountingFactory(load,operationId);
        }
    }


    private class ReferenceCountedJoinTable implements JoinTable{
        private final JoinTable delegate;
        private ReferenceCountingFactory refFactory;

        public ReferenceCountedJoinTable(JoinTable delegate,ReferenceCountingFactory refFactory){
            this.delegate=delegate;
            this.refFactory=refFactory;
        }

        @Override
        public Iterator<ExecRow> fetchInner(ExecRow outer) throws IOException, StandardException{
            return delegate.fetchInner(outer);
        }

        @Override
        public void close(){
            refFactory.markClosed();
            delegate.close();
        }
    }

    private class ReferenceCountingFactory implements JoinTable.Factory{
        private final JoinTable.Factory delegate;
        private final Long id;
        private AtomicInteger refCount = new AtomicInteger(0);

        public ReferenceCountingFactory(JoinTable.Factory delegate,Long id){
            this.delegate=delegate;
            this.id=id;
        }

        @Override
        public JoinTable newTable(){
            return new ReferenceCountedJoinTable(delegate.newTable(),this);
        }

        public void markClosed(){
            int refC=refCount.decrementAndGet();
            if(refC<=0)
                cache.invalidate(id);
        }
    }
}
