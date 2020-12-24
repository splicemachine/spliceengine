/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.utils;

import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import splice.com.google.common.collect.Iterators;

import java.util.Iterator;

public class IteratorUtil {
    public static Iterator<ByteSlice> getIterator(byte[] array) {
        final Iterator<ByteSlice> byteSliceIterator;
        if (array != null) {
            MultiFieldDecoder dcd = MultiFieldDecoder.wrap(array);
            byteSliceIterator = new DecodingIterator(dcd) {
                @Override
                protected void advance(MultiFieldDecoder decoder) {
                    decoder.skip();
                }
            };
        } else {
            byteSliceIterator = Iterators.emptyIterator();
        }

        return new Iterator<ByteSlice>() {

            final Iterator<ByteSlice> it = byteSliceIterator;

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public ByteSlice next() {
                ByteSlice dSlice = it.next();
                byte[] data = Encoding.decodeBytesUnsorted(dSlice.array(), dSlice.offset(), dSlice.length());
                dSlice.set(data);
                return dSlice;
            }
        };
    }
}
