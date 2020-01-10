/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.stream;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Abstraction for using a Stream as an Iterator. This is useful when
 * interacting with libraries which do not necessarily know of the Stream abstraction (although
 * is hides exceptions, so it's not always the best idea to use this over re-writing the library code).
 *
 * @author Scott Fines
 *         Date: 12/19/14
 */
final class StreamIterator<T> implements Iterator<T> {
    private final Stream<T> stream;
    private T next;
    private boolean hasNextCalled = false;

    public StreamIterator(Stream<T> stream) {
        this.stream = stream;
    }

    /**
     * @see {@link java.util.Iterator#hasNext()}.
     *
     * @return true if another element is present on the stream.
     * @throws java.lang.RuntimeException The runtime exception closed is 1 of two types:
     * If the cause of the StreamException is a RuntimeException, then it will be thrown directly,
     * otherwise, a RuntimeException wrapping the cause of the Exception will be thrown.
     */
    @Override
    public boolean hasNext() {
        if(hasNextCalled) return next!=null;
        next = safeNext();
        hasNextCalled = true;
        return next!=null;
    }

    @Override
    public T next() {
        if(!hasNext()) throw new NoSuchElementException();
        hasNextCalled = false;
        return next;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Streams do not support the removal of elements");
    }


    /**************************************************************************************************************/
    /*private helper methods*/
    private T safeNext() {
        try {
            return stream.next();
        } catch (StreamException e) {
            Throwable cause = e.getCause();
            if(cause!=null){
                if(cause instanceof RuntimeException)
                    throw (RuntimeException)cause;
                else throw new RuntimeException(cause);
            }else
                throw new RuntimeException(e);
        }
    }

}
