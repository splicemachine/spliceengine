package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.junit.Test;

import java.util.Random;

import static com.splicemachine.utils.BitSets.newBitSet;
import static org.junit.Assert.*;


public class IndexTransformerTest {

    private static final KryoPool KRYO = SpliceKryoRegistry.getInstance();
    private static final int[] SRC_COL_TYPES = new int[]{StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID, StoredFormatIds.SQL_INTEGER_ID};
    private static final boolean[] INDEX_KEY_SORT_ORDER = new boolean[]{true, true, true, true};

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // unique
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void unique_NoSourceKeyColumns_UniqueWithDuplicateNulls() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeEmpty().encodeNext(2).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);
        int[] indexKeyMap = new int[]{0, -1, -1, -1};

        IndexTransformer idx = new IndexTransformer(true, true, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, new Integer[]{null});
    }

    @Test
    public void unique_NoSourceKeyColumns() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(111).encodeNext(2).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, -1, -1, -1};

        IndexTransformer idx = new IndexTransformer(true, false, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, true, 111);
    }

    @Test
    public void unique_NoSourceKeyColumns_TwoNullIndexColumns() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 2), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(111).encodeNext(333);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, 1, 2, 3};

        IndexTransformer idx = new IndexTransformer(true, false, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, true, 111, null, 333, null);
    }

    @Test
    public void unique_NoSourceKeyColumns_TwoNullIndexColumns_ReOrdered() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 2), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(111).encodeNext(333);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{3, 2, 1, 0};

        IndexTransformer idx = new IndexTransformer(true, false, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, true, null, 333, null, 111);
    }

    @Test
    public void unique_NoSourceKeyColumns_multipleNullValuesAtStartOfIndexRowKey() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 2), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(111).encodeNext(333);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{3, 1, 2, 0};

        IndexTransformer idx = new IndexTransformer(true, false, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, true, null, null, 333, 111);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // non-unique
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void nonUnique_NoKeyColumns() throws Exception {
        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(-11111).encodeNext(2).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, -1, -1, -1};

        IndexTransformer idx = new IndexTransformer(false, false, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, -11111);
    }

    @Test
    public void nonUnique_TwoFieldsNoKeyColumns() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(0, 1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(1111).encodeNext(2222).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = randomByteArray(30);

        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, 1, -1, -1};

        IndexTransformer idx = new IndexTransformer(false, false, "1.0", null, SRC_COL_TYPES, null, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, 1111, 2222);
    }

    @Test
    public void nonUnique_OneFieldsOneKeyColumn() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(2).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = Encoding.encode(1111);

        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, -1, -1, -1};
        int[] sourceKeyEncodingOrder = new int[]{0};
        int[] sourceKeyTypes = new int[]{StoredFormatIds.SQL_INTEGER_ID};

        IndexTransformer idx = new IndexTransformer(false, false, "1.0", sourceKeyEncodingOrder, sourceKeyTypes, new boolean[]{true}, indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, 1111);
    }

    @Test
    public void nonUnique_TwoIndexFieldsOneKeyColumnNullSourceKey() throws Exception {

        EntryEncoder srcValueEncoder = EntryEncoder.create(KRYO, 4, newBitSet(1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        srcValueEncoder.getEntryEncoder().encodeNext(2).encodeNext(3).encodeNext(4);
        byte[] srcValue = srcValueEncoder.encode();
        byte[] srcRowKey = Encoding.encode(1111);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, -1, -1, -1};

        IndexTransformer transformer = new IndexTransformer(false, false, "1.0", null, SRC_COL_TYPES, null,
                indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = transformer.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, new Integer[]{null});
    }

    @Test
    public void nonUnique_TwoIndexFields_OneKeyColumn_NullSourceKeyAscDescInfo() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(222222).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = Encoding.encode(111111);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, 1, -1, -1};
        int[] sourceKeyEncodingOrder = new int[]{0};

        IndexTransformer idx = new IndexTransformer(false, false, "1.0", sourceKeyEncodingOrder, SRC_COL_TYPES, null,
                indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, 111111, 222222);
    }

    @Test
    public void nonUnique_TwoIndexFields_OneKeyColumn() throws Exception {

        EntryEncoder row = EntryEncoder.create(KRYO, 4, newBitSet(1, 2, 3), newBitSet(0, 1, 2, 3), newBitSet(), newBitSet());
        row.getEntryEncoder().encodeNext(2222222).encodeNext(3).encodeNext(4);
        byte[] srcValue = row.encode();
        byte[] srcRowKey = Encoding.encode(1111111);
        KVPair srcKvPair = new KVPair(srcRowKey, srcValue);

        int[] indexKeyMap = new int[]{0, 1, -1, -1};
        int[] sourceKeyEncodingOrder = new int[]{0};

        IndexTransformer idx = new IndexTransformer(false, false, "1.0", sourceKeyEncodingOrder, SRC_COL_TYPES, new boolean[]{true},
                indexKeyMap, INDEX_KEY_SORT_ORDER);

        KVPair indexKV = idx.translate(srcKvPair);

        verifyResultingIndexRowKey(indexKV.getRow(), srcRowKey, false, 1111111, 2222222);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // help
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /**
     * INDEX ROW KEY VERIFICATION:
     *
     * Verify index row key resulting from transformation has the expected column values
     * and, if unique, has the source row key appended.
     */
    public void verifyResultingIndexRowKey(byte[] indexRowKey, byte[] srcRowKey, boolean uniqueIndexAndRow, Integer... expectedIndexRowKeyValues) {
        assertNotNull("No row key set!", indexRowKey);
        assertTrue("No bytes in the row key!", indexRowKey.length > 0);

        MultiFieldDecoder keyDecoder = MultiFieldDecoder.wrap(indexRowKey);
        for (Integer expectedIndex : expectedIndexRowKeyValues) {
            if (expectedIndex != null) {
                assertEquals("index row key did cont contain expected value=" + expectedIndex, expectedIndex.intValue(), keyDecoder.decodeNextInt());
            } else {
                assertTrue(keyDecoder.nextIsNull());
                keyDecoder.skipLong();
            }
        }
        if (!uniqueIndexAndRow) {
            assertArrayEquals("Incorrect row reference!", srcRowKey, keyDecoder.decodeNextBytesUnsorted());
        } else {
            assertFalse("Expected to have read the entire index row key", keyDecoder.available());
        }
    }

    private static final Random RANDOM = new Random();

    private byte[] randomByteArray(int len) {
        byte[] b = new byte[len];
        RANDOM.nextBytes(b);
        return b;
    }

}
