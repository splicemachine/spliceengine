/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
