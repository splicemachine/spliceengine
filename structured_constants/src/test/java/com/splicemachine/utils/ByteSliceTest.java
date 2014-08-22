package com.splicemachine.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteSliceTest {

    @Test
    public void set_zeroLength_zeroOffset() {
        byte[] bytes = new byte[]{0, 1, 2, 4, 5, 6, 7, 8, 9, 10};
        ByteSlice byteSlice = new ByteSlice();

        byteSlice.set(bytes, 0, 0);

        assertEquals(0, byteSlice.getByteCopy().length);
        assertEquals(0, byteSlice.length());
        assertEquals(0, byteSlice.offset());
    }

    @Test
    public void set_zeroLength_NonZeroOffset() {
        byte[] bytes = new byte[]{0, 1, 2, 4, 5, 6, 7, 8, 9, 10};
        ByteSlice byteSlice = new ByteSlice();

        byteSlice.set(bytes, 5, 0);

        assertEquals(0, byteSlice.getByteCopy().length);
        assertEquals(0, byteSlice.length());
        assertEquals(5, byteSlice.offset());
    }

}