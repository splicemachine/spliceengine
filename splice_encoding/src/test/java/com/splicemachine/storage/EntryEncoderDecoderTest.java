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

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;
import com.carrotsearch.hppc.BitSet;


/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class EntryEncoderDecoderTest {
    private static KryoPool defaultPool = new KryoPool(100);
    @Test
    public void testGetData() throws Exception {
        BitSet setCols = new BitSet();
        setCols.set(0);
        setCols.set(1);

        BitSet scalarFields = new BitSet();
        scalarFields.set(0);
        scalarFields.set(1);
        EntryEncoder encoder = EntryEncoder.create(defaultPool,3, setCols, scalarFields, null, null);

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
        EntryEncoder encoder = EntryEncoder.create(defaultPool,3,setCols,scalarFields,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.setRawBytes((byte[])null);
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
        EntryEncoder encoder = EntryEncoder.create(defaultPool,3,setCols,scalarFields,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.setRawBytes((byte[])null);
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
        EntryEncoder encoder = EntryEncoder.create(defaultPool,3,setCols,scalarFields,null,null);

        MultiFieldEncoder entryEncoder = encoder.getEntryEncoder();
        entryEncoder.encodeNext(1);
        entryEncoder.encodeNext(2);
        entryEncoder.setRawBytes((byte[])null);
        byte[] encode = encoder.encode();

        EntryDecoder decoder = new EntryDecoder();
        decoder.set(encode);

        Assert.assertEquals(1, Encoding.decodeInt(decoder.getData(0)));
        Assert.assertEquals(2, Encoding.decodeInt(decoder.getData(1)));
        Assert.assertEquals(0,decoder.getData(2).length);
    }

}
