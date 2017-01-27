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

package com.splicemachine.derby.stream.iterator;

import com.splicemachine.derby.stream.iapi.PartitionAwareIterator;

/**
 * Created by jleach on 4/28/15.
 */
public class PartitionAwarePushBackIterator<T> implements PartitionAwareIterator<T> {
    private final PartitionAwareIterator<T> iterator;
    private final T EMPTY = (T) new Object();
    private T pushedBack = EMPTY;

    public PartitionAwarePushBackIterator(PartitionAwareIterator<T> iterator){
        this.iterator = iterator;
    }

    @Override
    public T next() {
        if (pushedBack != EMPTY) {
            T next = pushedBack;
            pushedBack = EMPTY;
            return next;
        }
        return iterator.next();
    }

    public void pushBack(T value){
        if (pushedBack != EMPTY) {
            throw new RuntimeException("Cannot push back multiple values.");
        }
        pushedBack = value;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public byte[] getPartition() {
        return iterator.getPartition();
    }
}

