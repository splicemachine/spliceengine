package com.splicemachine.utils;

import java.util.concurrent.ExecutionException;
import com.carrotsearch.hppc.ObjectArrayList;

/**
 * 
 * A Stripped down non-thread safe buffer.
 * 
 *
 * @author Scott Fines
 * Created on: 7/25/13
 */
public class UnsafeRingBuffer<E> {
    private ObjectArrayList<E> buffer;
    private int bufferSize;
    private Filler<E> filler;
    private int readSize;
    private boolean prep = true;
    private int exhaustSize;

    public UnsafeRingBuffer(int bufferSize, Filler<E> filler) {
    	this.bufferSize = bufferSize;
        this.filler = filler;
        this.buffer = new ObjectArrayList<E>(bufferSize);
        readSize = bufferSize+0;
        exhaustSize = bufferSize+0;
    }

	/**
     * Gets the next entry in the buffer. This method may block if the buffer needs to be refilled, but is
     * otherwise nonblocking.
     *
     * @return the next entry in the buffer, or {@code null} if there are no more entries to be returned.
     * @throws ExecutionException if something goes wrong while reading from the buffer.
     */
    public E next() throws ExecutionException {
        	if (readSize >= bufferSize) {
        		if (filler.isExhausted())
        			return null; // Exhausted
        		try {
	        		// Fill
        			filler.prepareToFill();
	        		for (int i = 0; i<bufferSize; i++) {
	        			if (prep)
	        				buffer.add(filler.getNext(null));
	        			else
	        				buffer.set(i, filler.getNext(buffer.get(i)));
	        			if (filler.isExhausted()) {
	        				exhaustSize=i;
	        				break;
	        			}
	        		}
        		} finally {
        			prep = false;
        			readSize = 0;
        			filler.finishFill();
        		}
        	}
        	try {
        		if (readSize > exhaustSize)
        			return null;
        		return buffer.get(readSize);
        	} finally {
            	readSize++;        		
        	}        	
    }
}
  
