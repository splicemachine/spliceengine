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

package com.splicemachine.derby.impl.sql.execute.operations;

import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.pipeline.Exceptions;
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

    public JoinTable.Factory get(Long operationId,
                         Callable<Stream<ExecRow>> loader,
                         int[] rightHashKeys,
                         int[] leftHashKeys,
                         ExecRow leftTemplateRow) throws IOException, StandardException{
        try{
            Loader callable=new Loader(operationId,tableLoader,rightHashKeys,leftHashKeys,leftTemplateRow,loader);
            ReferenceCountingFactory joinTable=cache.get(operationId,callable);
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


    private static class ReferenceCountedJoinTable implements JoinTable{
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
