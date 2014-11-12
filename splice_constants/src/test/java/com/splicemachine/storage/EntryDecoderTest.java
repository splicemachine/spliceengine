package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;
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

}