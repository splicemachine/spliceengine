package com.splicemachine.hbase.writer;

import com.carrotsearch.hppc.ObjectArrayList;

/**
 * @author Scott Fines
 *         Created on: 8/19/13
 */
public class ListenerRecordingCallBuffer<E> implements RecordingCallBuffer<E> {
    private final CallBuffer<E> delegate;

    private long totalElementsAdded = 0l;
    private long totalBytesAdded = 0l;
    private long totalFlushes = 0l;
    private final Listener<E> listener;

    public ListenerRecordingCallBuffer(CallBuffer<E> delegate, Listener<E> listener) {
        this.delegate = delegate;
        this.listener = listener;
    }

    public ListenerRecordingCallBuffer(CallBuffer<E> delegate){
        this(delegate,null);
    }

    @Override
    public void add(E element) throws Exception {
        totalElementsAdded+=1;
        totalBytesAdded+=listener.heapSize(element);
        delegate.add(element);
    }

    @Override
    public void addAll(E[] elements) throws Exception {
        delegate.addAll(elements);
    }

    @Override
    public void addAll(ObjectArrayList<E> elements) throws Exception {
        delegate.addAll(elements);
    }

    @Override
    public void flushBuffer() throws Exception {
        totalFlushes++;
        delegate.flushBuffer();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    @Override
    public long getTotalElementsAdded(){
        return totalElementsAdded;
    }

    @Override
    public long getTotalBytesAdded(){
        return totalBytesAdded;
    }

    @Override
    public long getTotalFlushes(){
        return totalFlushes;
    }

    @Override
    public double getAverageEntriesPerFlush(){
        return totalElementsAdded/(double)totalFlushes;
    }

    @Override
    public double getAverageSizePerFlush(){
        return totalBytesAdded/(double)totalFlushes;
    }

    @Override
    public CallBuffer<E> unwrap(){
        return delegate;
    }
}
