package com.splicemachine.derby.utils.marshall;

import com.splicemachine.encoding.debug.BitFormat;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SpreadBucketTest {

    private static final BitFormat bitFormat = new BitFormat(false);

    private SpreadBucket bucket = SpreadBucket.SIXTEEN;

    @Test
    public void bucketIndex() throws Exception {
        assertEquals(8, bucket.bucketIndex((byte) -128));
        assertEquals(0, bucket.bucketIndex((byte) 0x00));
        assertEquals(1, bucket.bucketIndex((byte) 0x10));
        assertEquals(2, bucket.bucketIndex((byte) 0x20));
        assertEquals(3, bucket.bucketIndex((byte) 0x30));
        assertEquals(4, bucket.bucketIndex((byte) 0x40));
        assertEquals(5, bucket.bucketIndex((byte) 0x50));
        assertEquals(6, bucket.bucketIndex((byte) 0x60));
        assertEquals(7, bucket.bucketIndex((byte) 0x70));
        assertEquals(8, bucket.bucketIndex((byte) 0x80));
        assertEquals(9, bucket.bucketIndex((byte) 0x90));
        assertEquals(10, bucket.bucketIndex((byte) 0xA0));
        assertEquals(11, bucket.bucketIndex((byte) 0xB0));
        assertEquals(12, bucket.bucketIndex((byte) 0xC0));
        assertEquals(13, bucket.bucketIndex((byte) 0xD0));
        assertEquals(14, bucket.bucketIndex((byte) 0xE0));
        assertEquals(15, bucket.bucketIndex((byte) 0xF0));
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