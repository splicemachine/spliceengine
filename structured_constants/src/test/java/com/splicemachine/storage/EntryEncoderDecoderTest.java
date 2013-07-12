package com.splicemachine.storage;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class EntryEncoderDecoderTest {

    @Test
    public void testGetData() throws Exception {
        EntryEncoder encoder = EntryEncoder.create(2, null, null,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(2);
        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertEquals(1, Encoding.decodeInt(decoder.getData(0)));
        Assert.assertEquals(2, Encoding.decodeInt(decoder.getData(1)));
    }

    @Test
    public void testGetDataWithNull() throws Exception {
        EntryEncoder encoder = EntryEncoder.create(3, null,null,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.setRawBytes(null);
        entryEncoder.encodeNext(2);
        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertEquals(1, Encoding.decodeInt(decoder.getData(0)));
        Assert.assertEquals(0,decoder.getData(1).length);
        Assert.assertEquals(2, Encoding.decodeInt(decoder.getData(2)));
    }

    @Test
    public void testGetDataWithHeadNull() throws Exception {
        EntryEncoder encoder = EntryEncoder.create(3, null,null,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.setRawBytes(null);
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(2);
        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertEquals(0,decoder.getData(0).length);
        Assert.assertEquals(1, Encoding.decodeInt(decoder.getData(1)));
        Assert.assertEquals(2, Encoding.decodeInt(decoder.getData(2)));
    }

    @Test
    public void testGetDataWithTailNull() throws Exception {
        EntryEncoder encoder = EntryEncoder.create(3, null,null,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(2);
        entryEncoder.setRawBytes(null);
        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertEquals(1, Encoding.decodeInt(decoder.getData(0)));
        Assert.assertEquals(2, Encoding.decodeInt(decoder.getData(1)));
        Assert.assertEquals(0,decoder.getData(2).length);
    }

}
