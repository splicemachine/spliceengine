package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraints;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.TableWriter;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.Collection;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 5/30/13
 */
public class LocalCallBuffer implements CallBuffer<Mutation> {
    public interface FlushListener{
        void finished(Map<Mutation,MutationResult> results) throws Exception;
    }
    private final WriteContext localCtx;
    private final long maxHeapSize;
    private final int maxBufferEntries;
    private long currentHeapSize = 0l;
    private int currentBufferSize = 0;
    private final FlushListener flushWatcher;

    public LocalCallBuffer(WriteContext localCtx,
                           long maxHeapSize,
                           int maxBufferEntries, FlushListener flushWatcher) {
        this.localCtx = localCtx;
        this.maxHeapSize = maxHeapSize;
        this.flushWatcher = flushWatcher;

        int bufferEntrySize = maxBufferEntries;
        if(bufferEntrySize<0)
            bufferEntrySize = Integer.MAX_VALUE;
        this.maxBufferEntries = bufferEntrySize;
    }

    public static LocalCallBuffer create(WriteContext context,FlushListener flushWatcher){
        //get the buffer size from the TableWriter
        TableWriter writer = SpliceDriver.driver().getTableWriter();
        long maxBufferSize = writer.getMaxBufferHeapSize();
        int maxEntries = writer.getMaxBufferEntries();

        return new LocalCallBuffer(context,maxBufferSize,maxEntries, flushWatcher);
    }

    @Override
    public void add(Mutation element) throws Exception {
        localCtx.sendUpstream(element);
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
        Map<Mutation,MutationResult> finish = localCtx.finish();
        flushWatcher.finished(finish);
        currentBufferSize=0;
        currentHeapSize = 0l;
    }


    @Override
    public void close() throws Exception {
        flushBuffer();
    }
}
