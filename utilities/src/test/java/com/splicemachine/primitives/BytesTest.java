package com.splicemachine.primitives;

import org.junit.Test;
import java.util.Random;
import static org.junit.Assert.*;

/**
 * Created by jleach on 11/11/15.
 */
public class BytesTest {

    @Test
    public void toHex_fromHex() {
        byte[] bytesIn = new byte[1024];
        new Random().nextBytes(bytesIn);

        String hex = Bytes.toHex(bytesIn);
        byte[] bytesOut = Bytes.fromHex(hex);

        assertArrayEquals(bytesIn, bytesOut);
    }

    @Test
    public void bytesToLong() {
        long longIn = new Random().nextLong();
        byte[] bytes = Bytes.longToBytes(longIn);
        long longOut = Bytes.bytesToLong(bytes, 0);
        assertEquals(longIn, longOut);
    }
}
