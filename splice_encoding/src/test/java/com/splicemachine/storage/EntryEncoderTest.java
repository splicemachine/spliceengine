/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
