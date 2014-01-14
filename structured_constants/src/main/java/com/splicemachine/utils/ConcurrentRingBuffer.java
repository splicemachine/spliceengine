package com.splicemachine.utils;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A Circular (Ring) Buffer of entries which is concurrent.
 *
 * This implementation is <em>amortized nonblocking</em>. That is, at any given
 * point in time, a call to next() will likely not block, but periodically blocking
 * may be necessary to preserve correctness. In this case, blocking is necessary whenever
 * the buffer is empty and needs to be refilled.
 *
 * Typically, a RingBuffer would be implemented using volatile variables and something like a CopyOnWriteArrayList
 * to act as a buffer collection. This type of implementation is relatively simple and does not
 * block (except during construction of the Buffer), but has the annoying side effect of constructing, then
 * destroying, a large number of potentially large arrays (depending on buffer size), which hurts the garbage
 * collector.
 *
 * To avoid excessive GC usage, this implementation reuses the buffer and (potentially) reuses the entries in the buffer
 * (if the filler deems that possible). To do this, it uses a counting scheme where the filler position is always kept
 * ahead of the reader position.
 *
 * The concept is this. There are two monotonically increasing counters--a write position counter, and a read position
 * counter. The write position counter keeps the last position that the writer reaches, and the read counter keeps
 * the last position ANY reader reaches. When the read counter catches up to the write counter, then the thread
 * which discovers this will know that the current entries in the buffer have been exhausted, and the buffer needs
 * to be refilled. It will then synchronize to block other threads from attempting to fill the buffer as well, and
 * then act to fill the buffer (increasing the writer position in the process). Once completed, the buffer has been
 * refilled and reads can resume.
 *
 * To prevent the writer from overwriting data that may not be completely read before, a reference array is kept which
 * is marked to indicate that a position is currently being read. The writer then knows it can only refill the buffer
 * up to the smallest position still being read. This may cause the writer to fill less than the total buffer,but will
 * retain correctness even in the face of many readers.
 *
 *
 * @author Scott Fines
 * Created on: 7/25/13
 */
public class ConcurrentRingBuffer<E> {

    private final AtomicInteger currentReadPos;
    private final AtomicInteger currentWritePos;
    private volatile boolean finished;
    private final E[] buffer;
    private final AtomicIntegerArray readingValues;
    private final int bufferSize;

    private volatile Filler<E> filler;


    public ConcurrentRingBuffer(int bufferSize, E[] bufferTemplate, Filler<E> filler) {
        //we want to compute the nearest power of 2, because arrays of 2^N are
        //more efficient to take modulus of
        bufferSize=(1<<(32-Integer.numberOfLeadingZeros(bufferSize)+1));
        this.currentReadPos = new AtomicInteger(0);
        this.currentWritePos = new AtomicInteger(0);
        this.bufferSize = bufferSize;
        this.filler = filler;

        //create a buffer of the proper size
        this.buffer = Arrays.copyOf(bufferTemplate,bufferSize);
        this.readingValues = new AtomicIntegerArray(bufferSize);
        for(int i=0;i<readingValues.length();i++){
            readingValues.set(i,-1);
        }
    }

    /**
     * Gets the next entry in the buffer. This method may block if the buffer needs to be refilled, but is
     * otherwise nonblocking.
     *
     * @return the next entry in the buffer, or {@code null} if there are no more entries to be returned.
     * @throws ExecutionException if something goes wrong while reading from the buffer.
     */
    public E next() throws ExecutionException {
        int bufferReadPos;
        int bufferWritePos;
        boolean fixedReadPos;
        do{
            bufferWritePos = currentWritePos.get();
            bufferReadPos = currentReadPos.get();
            if(bufferReadPos>=bufferWritePos){
                if(finished)
                    return null;
                else{
                    fillBuffer(bufferWritePos);
                }
            }else{
                int bufferPosition=bufferReadPos%bufferSize;
                fixedReadPos = readingValues.compareAndSet(bufferPosition,-1,1);
                if(fixedReadPos){
                    try{
                        int nextReadPos = bufferReadPos+1;
                        fixedReadPos = currentReadPos.compareAndSet(bufferReadPos,nextReadPos);
                        if(fixedReadPos){
                            return buffer[bufferPosition];
                        }
                    }finally{
                       readingValues.set(bufferPosition,-1);
                    }
                }
            }
        }while(true);
    }

    private void fillBuffer(int accessedWritePosition) throws ExecutionException {
        synchronized (buffer){
            int writePos = currentWritePos.get();
            if(writePos>accessedWritePosition) return;
            else if(finished) return;


            int smallestOutstandingPosition = -1;
            for(int i=0;i<readingValues.length();i++){
                if(readingValues.get(i)>0){
                    smallestOutstandingPosition=i;
                    break;
                }
            }
            int writePosition = writePos%bufferSize;
            int numEntriesToFill;
            if(writePosition<smallestOutstandingPosition){
                //can only go as far as the first reading position
                numEntriesToFill = smallestOutstandingPosition-writePosition;
            }else if(writePosition==smallestOutstandingPosition){
                /*
                 * We can't do anything, because there are readers directly in front of us.
                 * The only thing we can do is break, and loop back for another try after
                 * the readers are finished
                 */
                return;
            }else{
                //we can loop back as far as smallestOutstandingPosition
                numEntriesToFill = bufferSize-writePosition;
                if(smallestOutstandingPosition>=0)
                    numEntriesToFill-=smallestOutstandingPosition;
            }

            filler.prepareToFill();
            try{
                int pos;
                for(pos=0;pos<numEntriesToFill&&!finished;pos++){
                    int position = (writePos+pos)%bufferSize;
                    E old = buffer[position];
                    E newEntry = filler.getNext(old);
                    finished = newEntry==null;
                    if(!finished &&newEntry!=old){
                        /*
                         * We only need to change the buffer if the object is actually changes.
                         * If E is mutable, you can reuse objects safely this way as long as they are treated
                         * as thread-safe elsewhere.
                         */
                        buffer[position] = newEntry;
                    }
                }
                currentWritePos.set(writePos+pos);
            }finally{
                filler.finishFill();
            }
        }
    }
}
