package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 * Created on: 6/13/13
 */
public class MultiFieldDecoderTest {
    @Test
    public void testCanDecodeByteBufferCorrectly() throws Exception {
        String value = "testing";
        MultiFieldDecoder decoder = MultiFieldDecoder.create();
        decoder.set(Encoding.encode(value));

        ByteBuffer buffer = decoder.getNextRaw();
        Assert.assertEquals(value,Encoding.decodeString(buffer));
    }
}
