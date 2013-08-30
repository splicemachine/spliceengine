package com.splicemachine.hbase.writer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Thread-unsafe implementation of a CallBuffer. Useful for single-threaded operations,
 * where thread-safety isn't important when writing to the buffer.
 *
 * @author Scott Fines
 * Created on: 3/18/13
 */
public class UnsafeCallBuffer<E> implements CallBuffer<E>{
    private static final int BUFFER_SIZE_CHECK=10;
    private final List<E> buffer;
    private final Listener<E> listener;
    private long currentHeapSize;

    private final BufferConfiguration bufferConfiguration;

    public UnsafeCallBuffer(BufferConfiguration bufferConfiguration,Listener<E> listener) {
        this.listener = listener;
        this.bufferConfiguration = bufferConfiguration;

        if(bufferConfiguration.getMaxEntries()>0)
            buffer = new ArrayList<E>(bufferConfiguration.getMaxEntries());
        else
            buffer = new ArrayList<E>();
    }

    @Override
    public void add(E element) throws Exception {

        currentHeapSize+=listener.heapSize(element);
        buffer.add(element);

        checkBuffer();
    }

    @Override
    public void addAll(E[] elements) throws Exception {
        int n=0;
        for(E element:elements){
            n++;
            currentHeapSize+=listener.heapSize(element);
            buffer.add(element);

            if(n%BUFFER_SIZE_CHECK==0){
                checkBuffer();
            }
        }
        checkBuffer();
    }

    @Override
    public void addAll(Collection<? extends E> elements) throws Exception {
        int n=0;
        for(E element:elements){
            n++;
            currentHeapSize+=listener.heapSize(element);
            buffer.add(element);

            if(n%BUFFER_SIZE_CHECK==0){
                checkBuffer();
            }
        }
        checkBuffer();
    }

    @Override
    public void flushBuffer() throws Exception {
        if(buffer.size()<=0) return; //nothing to do

        List<E> elements = new ArrayList<E>(buffer);
        buffer.clear();
        currentHeapSize=0l;
        listener.bufferFlushed(elements,this);
    }

    @Override
    public void close() throws Exception {
        flushBuffer();
    }

    private void checkBuffer() throws Exception {
        int maxEntries = bufferConfiguration.getMaxEntries();
        if(currentHeapSize>bufferConfiguration.getMaxHeapSize()||(maxEntries>0 && buffer.size()>maxEntries))
            flushBuffer();
    }
}
