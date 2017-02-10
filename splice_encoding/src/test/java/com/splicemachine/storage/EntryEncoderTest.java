/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import java.math.BigDecimal;
import com.carrotsearch.hppc.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class EntryEncoderTest {
    private static KryoPool defaultPool = new KryoPool(100);
    @Test
    public void testCanEncodeAndDecodeCorrectlyCompressed() throws Exception {
        BitSet setBits = new BitSet(10);
        setBits.set(1);
        setBits.set(3);
        setBits.set(8);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,10, setBits, null, null, null);

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
        EntryEncoder encoder = EntryEncoder.create(defaultPool,10,setBits,null,null,null);

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
        EntryEncoder encoder = EntryEncoder.create(defaultPool,3, setCols,scalarFields,null,null);
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
