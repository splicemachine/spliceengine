package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

/**
 * Iterator which is allowed to throw exceptions.
 *
 * @author Scott Fines
 * Created on: 11/1/13
 */
public interface StandardIterator<T> {

    /**
     * Open the iterator for operations. Should be called
     * before the first call to next();
     *
     * @throws StandardException
     * @throws IOException
     */
    void open() throws StandardException, IOException;

    /**
     * Get the next element in the interation
     * @return the next element in the iteration, or {@code null} if no
     * more elements exist
     *
     * @throws StandardException
     * @throws IOException
     */
    T next() throws StandardException,IOException;

    /**
     * Close the iterator. Should be called after the last
     * call to next(); once called, {@link #next()} should
     * no longer be called.
     *
     * @throws StandardException
     * @throws IOException
     */
    void close() throws StandardException,IOException;
}
