/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
