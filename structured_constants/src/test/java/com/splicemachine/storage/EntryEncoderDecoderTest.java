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
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet();
        scalarFields.set(0);
        scalarFields.set(1);
        EntryEncoder encoder = EntryEncoder.create(3,setCols,scalarFields,null,null);

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
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);
        setCols.set(2);

        BitSet scalarFields = new BitSet();
        scalarFields.set(0);
        scalarFields.set(2);
        EntryEncoder encoder = EntryEncoder.create(3,setCols,scalarFields,null,null);

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
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);
        setCols.set(2);

        BitSet scalarFields = new BitSet();
        scalarFields.set(1);
        scalarFields.set(2);
        EntryEncoder encoder = EntryEncoder.create(3,setCols,scalarFields,null,null);

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
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);
        setCols.set(2);

        BitSet scalarFields = new BitSet();
        scalarFields.set(1);
        scalarFields.set(2);
        EntryEncoder encoder = EntryEncoder.create(3,setCols,scalarFields,null,null);

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
