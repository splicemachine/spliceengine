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
