package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class EntryEncoderTest {

    @Test
    public void testCanEncodeAndDecodeCorrectlyCompressed() throws Exception {
        BitSet setBits = new BitSet(10);
        setBits.set(1);
        setBits.set(3);
        setBits.set(8);
        EntryEncoder encoder = EntryEncoder.create(10,setBits,null,null,null);

        String longTest = "hello this is a tale of woe and sadness from which we will never returnaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbeeeeeeeeeeeeeeeeeeeeeeeeeeeeea";
        BigDecimal correct = new BigDecimal("22.456789012345667890230456677890192348576");
        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(longTest);
        entryEncoder.encodeNext(correct);

        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertTrue(decoder.isSet(1));
        Assert.assertTrue(decoder.isSet(3));
        Assert.assertTrue(decoder.isSet(8));
        Assert.assertFalse(decoder.isSet(4));


        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();

        Assert.assertEquals(1,fieldDecoder.decodeNextInt());
        Assert.assertEquals(longTest,fieldDecoder.decodeNextString());
        BigDecimal next = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("expected: "+ correct+", actual: "+ next,correct.compareTo(next)==0);

    }

    @Test
    public void testCanEncodeAndDecodeCorrectlyUncompressed() throws Exception {
        BitSet setBits = new BitSet(10);
        setBits.set(1);
        setBits.set(3);
        setBits.set(8);
        EntryEncoder encoder = EntryEncoder.create(10,setBits,null,null,null);

        String longTest = "hello this is a tale of woe and sadness from which we will never return";
        BigDecimal correct = new BigDecimal("22.456789012345667890230456677890192348576");
        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(longTest);
        entryEncoder.encodeNext(correct);

        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertTrue(decoder.isSet(1));
        Assert.assertTrue(decoder.isSet(3));
        Assert.assertTrue(decoder.isSet(8));
        Assert.assertFalse(decoder.isSet(4));


        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();

        Assert.assertEquals(1,fieldDecoder.decodeNextInt());
        Assert.assertEquals(longTest,fieldDecoder.decodeNextString());
        BigDecimal next = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("expected: "+ correct+", actual: "+ next,correct.compareTo(next)==0);

    }

    @Test
    public void testEncodeAllColumnsSafely() throws Exception {
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);
        setCols.set(2);

        BitSet scalarFields = new BitSet();
        scalarFields.set(1);
        EntryEncoder encoder = EntryEncoder.create(3, setCols,scalarFields,null,null);
        String longTest = "hello this is a tale of woe and sadness from which we will never return";
        BigDecimal correct = new BigDecimal("22.456789012345667890230456677890192348576");
        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(longTest);
        entryEncoder.encodeNext(correct);

        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertTrue(decoder.isSet(0));
        Assert.assertTrue(decoder.isSet(1));
        Assert.assertTrue(decoder.isSet(2));


        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();

        Assert.assertEquals(1,fieldDecoder.decodeNextInt());
        Assert.assertEquals(longTest,fieldDecoder.decodeNextString());
        BigDecimal next = fieldDecoder.decodeNextBigDecimal();
        Assert.assertTrue("expected: "+ correct+", actual: "+ next,correct.compareTo(next)==0);

    }
}
