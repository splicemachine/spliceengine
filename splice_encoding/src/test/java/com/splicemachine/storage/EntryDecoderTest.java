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

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EntryDecoderTest {

    @Test
    public void testBitSetConstructionAndStringDecoding() throws IOException {

        // <null, null, null, null, null, "BARBARBAR", "sjzoxub">
        byte[] value = new byte[]{-128, -92, 0, 68, 67, 84, 68, 67, 84, 68, 67, 84, 0, 117, 108, 124, 113, 122, 119, 100};

        EntryDecoder rowDecoder = new EntryDecoder(value);

        BitIndex bitIndex = rowDecoder.getCurrentIndex();
        MultiFieldDecoder rowFieldDecoder = rowDecoder.getEntryDecoder();

        assertEquals(2, bitIndex.cardinality());
        assertEquals("{{5, 6},{},{},{}}", bitIndex.toString());
        assertFalse(bitIndex.isSet(0));
        assertTrue(bitIndex.isSet(5));
        assertTrue(bitIndex.isSet(6));
        assertFalse(bitIndex.isSet(7));

        assertEquals("BARBARBAR", rowFieldDecoder.decodeNextString());
        assertEquals("sjzoxub", rowFieldDecoder.decodeNextString());
    }

    @Test
    public void getCurrentIndex_returnsIndexWithCardinalityOfNonNullFields() throws IOException {

        // < null, null, 'C' >
        byte[] value = new byte[]{-126, -128, 0, 69};
        EntryDecoder entryDecoder = new EntryDecoder(value);

        BitIndex bitIndex = entryDecoder.getCurrentIndex();

        assertEquals("Note that the cardinality only indicates number of non-null fields", 1, bitIndex.cardinality());
    }

    @Test
    public void rebuildBitIndexWorksProperlyForSpecificByteArray() throws Exception{
        byte[] value = new byte[]{0,-64,-128,0,51};
        EntryDecoder decoder = new EntryDecoder(value);

        BitIndex bi = decoder.getCurrentIndex();
        BitSet correct = new BitSet();
        correct.set(128);
        BitIndex correctIndex =BitIndexing.sparseBitMap(correct,new BitSet(),new BitSet(),new BitSet());

        Assert.assertEquals("Incorrect decoded bit index!",correctIndex,bi);
    }
}