package com.splicemachine.utils;
import java.util.concurrent.ExecutionException;

public interface Filler<E>{

    /**
     * Signifies that a buffer fill is being prepared, and it is time to acquire locks or otherwise
     * set up for buffer filling.
     *
     * @throws ExecutionException if the fill fails to prepare properly.
     */
    void prepareToFill() throws ExecutionException;

    /**
     * Get the next entry to place into the buffer.
     * @param old the old (already read) version of the next buffer entry to fill.
     *            Useful if you want to reuse objects like lists.
     * @return the entry in the next position, or {@code null} if there are no
     * more entries to add to the buffer. If {@code old} is returned, then no action will
     * be taken, as {@code old} will remain in the buffer modified as desired.
     */
    E getNext(E old) throws ExecutionException;    
    
    /**
     * Called once the fill is completed to remove locks or otherwise clean up after a filling action.
     *
     * @throws ExecutionException the fill fails to complete successfully.
     */
    void finishFill() throws ExecutionException;
    
    /**
     * 
     * Is the filler exhausted
     * 
     */
    
    boolean isExhausted() throws ExecutionException;
}
