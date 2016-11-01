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