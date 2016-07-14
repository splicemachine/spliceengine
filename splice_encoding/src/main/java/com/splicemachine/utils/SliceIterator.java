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

package com.splicemachine.utils;

import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 8/14/14
 */
public class SliceIterator implements Iterator<ByteSlice> {
    private final Iterator<byte[]> iterator;
    private final ByteSlice slice = new ByteSlice();

    public SliceIterator(Iterator<byte[]> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public ByteSlice next() {
        slice.set(iterator.next());
        return slice;
    }

    @Override public void remove() { throw new UnsupportedOperationException(); }
}
