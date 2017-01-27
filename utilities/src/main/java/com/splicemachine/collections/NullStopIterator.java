/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
