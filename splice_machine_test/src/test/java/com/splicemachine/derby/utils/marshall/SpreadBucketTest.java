package com.splicemachine.derby.utils.marshall;

import com.splicemachine.encoding.debug.BitFormat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpreadBucketTest {

    private static final BitFormat bitFormat = new BitFormat(false);

    private SpreadBucket bucket16 = SpreadBucket.SIXTEEN;
    private SpreadBucket bucket32 = SpreadBucket.THIRTY_TWO;
    private SpreadBucket bucket64 = SpreadBucket.SIXTY_FOUR;
    private SpreadBucket bucket128 = SpreadBucket.ONE_TWENTY_EIGHT;
    @Test
    public void bucketIndex() throws Exception {
        assertEquals(8, bucket16.bucketIndex((byte) -128));
        assertEquals(0, bucket16.bucketIndex((byte) 0x00));
        assertEquals(1, bucket16.bucketIndex((byte) 0x10));
        assertEquals(2, bucket16.bucketIndex((byte) 0x20));
        assertEquals(3, bucket16.bucketIndex((byte) 0x30));
        assertEquals(4, bucket16.bucketIndex((byte) 0x40));
        assertEquals(5, bucket16.bucketIndex((byte) 0x50));
        assertEquals(6, bucket16.bucketIndex((byte) 0x60));
        assertEquals(7, bucket16.bucketIndex((byte) 0x70));
        assertEquals(8, bucket16.bucketIndex((byte) 0x80));
        assertEquals(9, bucket16.bucketIndex((byte) 0x90));
        assertEquals(10, bucket16.bucketIndex((byte) 0xA0));
        assertEquals(11, bucket16.bucketIndex((byte) 0xB0));
        assertEquals(12, bucket16.bucketIndex((byte) 0xC0));
        assertEquals(13, bucket16.bucketIndex((byte) 0xD0));
        assertEquals(14, bucket16.bucketIndex((byte) 0xE0));
        assertEquals(15, bucket16.bucketIndex((byte) 0xF0));
    }

    @Test
    public void bucketIndex32() throws Exception {
        assertEquals(0, bucket32.bucketIndex((byte) 0x00));
        assertEquals(1, bucket32.bucketIndex((byte) 0x08));
        assertEquals(2, bucket32.bucketIndex((byte) 0x10));
        assertEquals(3, bucket32.bucketIndex((byte) 0x18));
        assertEquals(4, bucket32.bucketIndex((byte) 0x20));
        assertEquals(5, bucket32.bucketIndex((byte) 0x28));
        assertEquals(6, bucket32.bucketIndex((byte) 0x30));
        assertEquals(7, bucket32.bucketIndex((byte) 0x38));
        assertEquals(8, bucket32.bucketIndex((byte) 0x40));
        assertEquals(9, bucket32.bucketIndex((byte) 0x48));
        assertEquals(10, bucket32.bucketIndex((byte) 0x50));
        assertEquals(11, bucket32.bucketIndex((byte) 0x58));
        assertEquals(12, bucket32.bucketIndex((byte) 0x60));
        assertEquals(13, bucket32.bucketIndex((byte) 0x68));
        assertEquals(14, bucket32.bucketIndex((byte) 0x70));
        assertEquals(15, bucket32.bucketIndex((byte) 0x78));
        assertEquals(16, bucket32.bucketIndex((byte) 0x80));
        assertEquals(17, bucket32.bucketIndex((byte) 0x88));
        assertEquals(18, bucket32.bucketIndex((byte) 0x90));
        assertEquals(19, bucket32.bucketIndex((byte) 0x98));
        assertEquals(20, bucket32.bucketIndex((byte) 0xa0));
        assertEquals(21, bucket32.bucketIndex((byte) 0xa8));
        assertEquals(22, bucket32.bucketIndex((byte) 0xb0));
        assertEquals(23, bucket32.bucketIndex((byte) 0xb8));
        assertEquals(24, bucket32.bucketIndex((byte) 0xc0));
        assertEquals(25, bucket32.bucketIndex((byte) 0xc8));
        assertEquals(26, bucket32.bucketIndex((byte) 0xd0));
        assertEquals(27, bucket32.bucketIndex((byte) 0xd8));
        assertEquals(28, bucket32.bucketIndex((byte) 0xe0));
        assertEquals(29, bucket32.bucketIndex((byte) 0xe8));
        assertEquals(30, bucket32.bucketIndex((byte) 0xf0));
        assertEquals(31, bucket32.bucketIndex((byte) 0xf8));
    }

    @Test
    public void bucketIndex64() throws Exception {
        assertEquals(0, bucket64.bucketIndex((byte) 0x00));
        assertEquals(1, bucket64.bucketIndex((byte) 0x04));
        assertEquals(2, bucket64.bucketIndex((byte) 0x08));
        assertEquals(3, bucket64.bucketIndex((byte) 0x0c));
        assertEquals(4, bucket64.bucketIndex((byte) 0x10));
        assertEquals(5, bucket64.bucketIndex((byte) 0x14));
        assertEquals(6, bucket64.bucketIndex((byte) 0x18));
        assertEquals(7, bucket64.bucketIndex((byte) 0x1c));
    }
    
    @Test
    public void bucketIndex128() throws Exception {
        assertEquals(0, bucket128.bucketIndex((byte) 0x00));
        assertEquals(1, bucket128.bucketIndex((byte) 0x02));
        assertEquals(2, bucket128.bucketIndex((byte) 0x04));
        assertEquals(3, bucket128.bucketIndex((byte) 0x06));
        assertEquals(4, bucket128.bucketIndex((byte) 0x08));
        assertEquals(5, bucket128.bucketIndex((byte) 0x0a));
        assertEquals(6, bucket128.bucketIndex((byte) 0x0c));
        assertEquals(7, bucket128.bucketIndex((byte) 0x0e));
        assertEquals(8, bucket128.bucketIndex((byte) 0x10));
    }
    
    @Test
    public void bucket() throws Exception {
        assertEquals("11000000", bitFormat.format(SpreadBucket.FOUR.bucket(Integer.MAX_VALUE)));
        assertEquals("11100000", bitFormat.format(SpreadBucket.EIGHT.bucket(Integer.MAX_VALUE)));
        assertEquals("11110000", bitFormat.format(SpreadBucket.SIXTEEN.bucket(Integer.MAX_VALUE)));
        assertEquals("11111000", bitFormat.format(SpreadBucket.THIRTY_TWO.bucket(Integer.MAX_VALUE)));
        assertEquals("11111100", bitFormat.format(SpreadBucket.SIXTY_FOUR.bucket(Integer.MAX_VALUE)));
        assertEquals("11111110", bitFormat.format(SpreadBucket.ONE_TWENTY_EIGHT.bucket(Integer.MAX_VALUE)));
        assertEquals("11111111", bitFormat.format(SpreadBucket.TWO_FIFTY_SIX.bucket(Integer.MAX_VALUE)));
    }

}
