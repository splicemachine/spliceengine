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
