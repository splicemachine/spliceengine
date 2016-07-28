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

