package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;

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

    @Test
    public void testCanDecodeNullEntryByteBuffer() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext("test");
        encoder.setRawBytes(new byte[]{});
        encoder.encodeNext("test2");

        MultiFieldDecoder decoder = MultiFieldDecoder.create();
        decoder.set(encoder.build());
        String val = decoder.decodeNextString();
        ByteBuffer buffer = decoder.getNextRaw();
        String val2 = decoder.decodeNextString();

        Assert.assertEquals("test",val);
        Assert.assertNull(buffer);
        Assert.assertEquals("test2",val2);
    }

    @Test
    public void testCanDecodeNullEntry() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext("test");
        encoder.setRawBytes(new byte[]{});
        encoder.encodeNext("test2");

        MultiFieldDecoder decoder = MultiFieldDecoder.create();
        decoder.set(encoder.build());
        String val = decoder.decodeNextString();
        byte[] bytes = decoder.decodeNextBytes();
        String val2 = decoder.decodeNextString();

        Assert.assertEquals("test",val);
        Assert.assertEquals(0,bytes.length);
        Assert.assertEquals("test2",val2);
    }

    @Test
    public void testCanDecodeBigDecimalInMiddle() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext("hello");
        encoder.encodeNext(BigDecimal.valueOf(25));
        encoder.encodeNext("goodbye");

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(encoder.build());
        Assert.assertEquals("hello",decoder.decodeNextString());
        Assert.assertEquals(BigDecimal.valueOf(25),decoder.decodeNextBigDecimal());
        Assert.assertEquals("goodbye",decoder.decodeNextString());
    }
}
