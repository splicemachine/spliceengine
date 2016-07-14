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

package com.splicemachine.collections;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Iterator which is designed around the idea that a stream will feed items until
 * exhausted, at which point it will feed null. Thus, it will capture elements in the
 * hashNext() method, then feed them in the next() method until next() is null.
 *
 * @author Scott Fines
 * Date: 7/29/14
 */
public abstract class NullStopIterator<T> implements CloseableIterator<T> {

    private T next;

    protected abstract T nextItem() throws IOException;

    @Override
    public boolean hasNext() {
        //still haven't fetched since the last call to this method
        if(next!=null) return true;

        try {
            next = nextItem();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return next!=null;
    }

    @Override
    public T next() {
        if(next==null) throw new NoSuchElementException();
        T n = next;
        next = null;
        return n;
    }

    @Override public void remove() { throw new UnsupportedOperationException(); }
}
