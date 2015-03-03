package com.splicemachine.intg.hive;

import java.io.IOException;

import com.splicemachine.db.iapi.error.StandardException;


public interface SpliceStandardIterator<T> {

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
