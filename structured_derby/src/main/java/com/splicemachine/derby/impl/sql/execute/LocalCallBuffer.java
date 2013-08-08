package com.splicemachine.derby.impl.sql.execute;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.writer.TableWriter;
import com.splicemachine.hbase.batch.WriteContext;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 * Created on: 5/30/13
 */
public class LocalCallBuffer implements CallBuffer<Mutation> {
    private volatile Exception error;

    public interface FlushListener{
        void finished(Map<Mutation,MutationResult> results) throws Exception;

    }
    private final LocalWriteContextFactory localCtxFactory;
    private final long maxHeapSize;
    private final int maxBufferEntries;
    private long currentHeapSize = 0l;
    private int currentBufferSize = 0;
    private final FlushListener flushWatcher;
    private final MonitoredThreadPool writerPool;
    private final List<Mutation> mutations;
    private final List<Future<Void>> writeFutures = Lists.newArrayList();
    private final RegionCoprocessorEnvironment rce;

    public LocalCallBuffer(LocalWriteContextFactory localCtxFactory,
                           RegionCoprocessorEnvironment rce,
                           long maxHeapSize,
                           int maxBufferEntries, FlushListener flushWatcher) {
        this.rce = rce;
        this.maxHeapSize = maxHeapSize;
        this.localCtxFactory = localCtxFactory;
        this.flushWatcher = flushWatcher;

        int bufferEntrySize = maxBufferEntries;
        if(bufferEntrySize<0){
            bufferEntrySize = Integer.MAX_VALUE;
            this.mutations = Lists.newArrayList();
        }else{
            this.mutations = Lists.newArrayListWithCapacity(bufferEntrySize);
        }
        this.maxBufferEntries = bufferEntrySize;
        //use the same thread pool that the TableWriter uses
        this.writerPool = SpliceDriver.driver().getTableWriter().getThreadPool();
    }

    public static LocalCallBuffer create(LocalWriteContextFactory context,RegionCoprocessorEnvironment rce,FlushListener flushWatcher){
        //get the buffer size from the TableWriter
        TableWriter writer = SpliceDriver.driver().getTableWriter();
        long maxBufferSize = writer.getMaxBufferHeapSize();
        int maxEntries = writer.getMaxBufferEntries();

        return new LocalCallBuffer(context,rce,maxBufferSize,maxEntries, flushWatcher);
    }

    @Override
    public void add(Mutation element) throws Exception {
        Exception e = error;
        if(e!=null){
            error=null; //reset, then throw
            throw e;
        }
        mutations.add(element);
        currentHeapSize+=(element instanceof Put)?((Put)element).heapSize(): element.getRow().length;
        currentBufferSize++;
        if(currentHeapSize>=maxHeapSize||currentBufferSize>=maxBufferEntries){
            flushBuffer();
        }
    }

    @Override
    public void addAll(Mutation[] elements) throws Exception {
        for(Mutation element:elements){
            add(element);
        }
    }

    @Override
    public void addAll(Collection<? extends Mutation> elements) throws Exception {
        for(Mutation element:elements){
            add(element);
        }
    }

    @Override
    public void flushBuffer() throws Exception {
        final List<Mutation> mutationsToWrite = Lists.newArrayList(mutations);
        currentBufferSize=0;
        currentHeapSize = 0l;
        writeFutures.add(writerPool.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                WriteContext localCtx = localCtxFactory.create(rce);
                for (Mutation mutation : mutationsToWrite) {
                    localCtx.sendUpstream(mutation);
                }
                try{
                    Map<Mutation, MutationResult> finish = localCtx.finish();
                    flushWatcher.finished(finish);
                }catch(IOException ioe){
                    error = ioe;
                }
                return null;
            }
        }));
    }


    @Override
    public void close() throws Exception {
        flushBuffer();
        //make sure that all our threads have completed successfully.
        for(Future<Void> future:writeFutures){
            future.get();
        }
    }
}
