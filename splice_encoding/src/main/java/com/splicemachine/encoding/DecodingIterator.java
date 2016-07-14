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

package com.splicemachine.encoding;

import com.splicemachine.utils.ByteSlice;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * Note: This class should NOT be used when there are scalar or
 * double fields to decode--it will only work correctly
 * when {@link MultiFieldDecoder#skip()} works correctly.
 *
 * @author Scott Fines
 * Date: 8/14/14
 */
public abstract class DecodingIterator implements Iterator<ByteSlice> {
    private final MultiFieldDecoder decoder;
    private final ByteSlice slice;

    public DecodingIterator(MultiFieldDecoder decoder) {
        this.decoder = decoder;
        this.slice = new ByteSlice();
    }

    @Override
    public boolean hasNext() {
        return decoder.available();
    }

    @Override
    public ByteSlice next() {
        int offset = decoder.offset();
        advance(decoder);
        int length = decoder.offset()-offset-1;
        if(length<=0) throw new NoSuchElementException();

        slice.set(decoder.array(),offset,length);
        return slice;
    }

    protected abstract void advance(MultiFieldDecoder decoder);

    @Override public void remove() { throw new UnsupportedOperationException(); }
}
