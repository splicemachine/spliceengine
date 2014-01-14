package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Scott Fines
 *         Created on: 10/3/13
 */
public class FixedByteEncodingTest {

    @Test
    public void testCanEncodeAndDecodeValueCorrectlyDescending() throws Exception {
        /*
         * Regression test for specific, found erronous value
         */

        byte[] data = new byte[]{-116,34,-81,77,103,-57};

        byte[] encoded = ByteEncoding.encode(data,true);
        byte[] decoded = ByteEncoding.decode(encoded,true);

        Assert.assertArrayEquals("Incorrect encode/decode!",data,decoded);

    }
}
