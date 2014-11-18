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

    @Test
    public void toHexString() {
        ByteSlice byteSlice = new ByteSlice();
        byteSlice.set(new byte[]{18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
        assertEquals("1211100F0E0D0C0B0A09080706050403020100", byteSlice.toHexString());
    }

    @Test
    public void find() {
        byte[] sourceBytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteSlice byteSlice = ByteSlice.wrap(sourceBytes);
        assertEquals(0, byteSlice.find((byte) 1, 0));
        assertEquals(1, byteSlice.find((byte) 2, 0));
        assertEquals(2, byteSlice.find((byte) 3, 0));

        sourceBytes = new byte[]{-1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, 120};
        byteSlice = ByteSlice.wrap(sourceBytes, 4, 10);
        assertEquals(0, byteSlice.find((byte) 0, 0));
        assertEquals(1, byteSlice.find((byte) 1, 0));
        assertEquals(2, byteSlice.find((byte) 2, 0));
        assertEquals(-1, byteSlice.find((byte) 120, 0));
    }


}