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
