package com.splicemachine.derby.impl.sql.execute.index;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.carrotsearch.hppc.BitSet;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.SparseEntryAccumulator;
import com.splicemachine.storage.index.BitIndex;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

/**
 * Class responsible for transforming an incoming main table row
 * into an index row
 *
 * @author Scott Fines
 *         Created on: 8/23/13
 */
public class IndexTransformer {
    private final BitSet indexedColumns;
    private final BitSet nonUniqueIndexedColumns;
    private final BitSet translatedIndexedColumns;
    private final boolean isUnique;
    private final boolean isUniqueWithDuplicateNulls;
    private final int[] mainColToIndexPosMap;
    private final BitSet descColumns;
    private EntryAccumulator indexKeyAccumulator;
    private EntryAccumulator indexRowAccumulator;
    private EntryDecoder mainPutDecoder;
    private int[] columnOrdering;
    private int[] format_ids;
    private KeyData[] pkIndex;
    private DataValueDescriptor[] kdvds;
    private MultiFieldDecoder keyDecoder;
    private BitSet pkColumns;
    private BitSet pkIndexColumns;
    private static final Logger LOG = Logger.getLogger(IndexTransformer.class);
    public IndexTransformer(BitSet indexedColumns,
                            BitSet translatedIndexedColumns,
                            BitSet nonUniqueIndexedColumns,
                            BitSet descColumns,
                            int[] mainColToIndexPosMap,
                            boolean isUnique,
                            boolean isUniqueWithDuplicateNulls) {
        this.indexedColumns = indexedColumns;
        this.translatedIndexedColumns = translatedIndexedColumns;
        this.nonUniqueIndexedColumns = nonUniqueIndexedColumns;
        this.descColumns = descColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
    }

    public IndexTransformer(BitSet indexedColumns,
                            BitSet translatedIndexedColumns,
                            BitSet nonUniqueIndexedColumns,
                            BitSet descColumns,
                            int[] mainColToIndexPosMap,
                            boolean isUnique,
                            boolean isUniqueWithDuplicateNulls,
                            int[] columnOrdering,
                            int[] format_ids){
        this.indexedColumns = indexedColumns;
        this.translatedIndexedColumns = translatedIndexedColumns;
        this.nonUniqueIndexedColumns = nonUniqueIndexedColumns;
        this.descColumns = descColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.columnOrdering = columnOrdering;
        this.format_ids = format_ids;
        pkColumns = new BitSet();
        if(columnOrdering != null && columnOrdering.length > 0) {
            for (int col:columnOrdering) {
                pkColumns.set(col);
            }
        }
    }

    public static IndexTransformer newTransformer(BitSet indexedColumns,
                                                  int[] mainColToIndexPosMap,
                                                  BitSet descColumns,
                                                  boolean unique,
                                                  boolean uniqueWithDuplicateNulls,
                                                  int[] columnOrdering,
                                                  int[] format_ids){
        BitSet translatedIndexedColumns = new BitSet(indexedColumns.cardinality());
        for (int i = indexedColumns.nextSetBit(0); i >= 0; i = indexedColumns.nextSetBit(i + 1)) {
            translatedIndexedColumns.set(mainColToIndexPosMap[i]);
        }
        BitSet nonUniqueIndexedColumns = (BitSet) translatedIndexedColumns.clone();
        nonUniqueIndexedColumns.set(translatedIndexedColumns.length());

        return new IndexTransformer(indexedColumns,
                                    translatedIndexedColumns,
                                    nonUniqueIndexedColumns,
                                    descColumns,
                                    mainColToIndexPosMap,
                                    unique,
                                    uniqueWithDuplicateNulls,
                                    columnOrdering,
                                    format_ids
        );
    }

    private void buildKeyMap (KVPair mutation) throws StandardException{

        // if index key column set and primary key column set do not intersect,
        // no need to build a key map
        pkIndexColumns = (BitSet)pkColumns.clone();
        pkIndexColumns.and(indexedColumns);
        if(pkIndexColumns.cardinality() > 0)
        {
            int len = columnOrdering.length;
            createKeyDecoder();
            if (kdvds == null) {
                kdvds = new DataValueDescriptor[len];
                for(int i = 0; i < len; ++i) {
                    kdvds[i] = LazyDataValueFactory.getLazyNull(format_ids[columnOrdering[i]]);
                }
            }
            if(pkIndex == null)
                pkIndex = new KeyData[len];
            keyDecoder.set(mutation.getRow());
            for (int i = 0; i < len; ++i) {
                int offset = keyDecoder.offset();
                DerbyBytesUtil.skip(keyDecoder, kdvds[i]);
                int size = keyDecoder.offset()-1-offset;
                pkIndex[i] = new KeyData(offset, size);
            }
        }
    }

    private void createKeyDecoder() {
        if (keyDecoder == null)
            keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
    }

    public KVPair translate(KVPair mutation) throws IOException, StandardException {
        if (mutation == null) return null; //nothing to do
        buildKeyMap(mutation);

        //make sure that row and key accumulators are initialized
        getRowAccumulator();
        getKeyAccumulator();

        indexKeyAccumulator.reset();
        indexRowAccumulator.reset();

        if (mainPutDecoder == null)
            mainPutDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

        mainPutDecoder.set(mutation.getValue());

        BitIndex mutationIndex = mainPutDecoder.getCurrentIndex();
        MultiFieldDecoder mutationDecoder = mainPutDecoder.getEntryDecoder();

        // Check for null columns in data when isUniqueWithDuplicateNulls == true
        // -- in this case, we'll have to append a uniqueness value to the row key
        boolean makeUniqueForDuplicateNulls = false;
        int pos = 0;
        for (int i = 0; i < format_ids.length; ++i) {
            if (pkIndexColumns.get(i)) {
                indexKeyAccumulator.add(mainColToIndexPosMap[i],
                        ByteBuffer.wrap(keyDecoder.array(),pkIndex[i].getOffset(),pkIndex[i].getSize()));
                //indexRowAccumulator.add(pos++, null);
            }
            else if (indexedColumns.get(i)) {
                if (isUniqueWithDuplicateNulls && !makeUniqueForDuplicateNulls) {
                    makeUniqueForDuplicateNulls = mainPutDecoder.nextIsNull(i);
                }
                ByteBuffer entry = mainPutDecoder.nextAsBuffer(mutationDecoder, i);
                if (descColumns.get(mainColToIndexPosMap[i]))
                    accumulate(indexKeyAccumulator, mutationIndex, getDescendingBuffer(entry), i);
                else
                    accumulate(indexKeyAccumulator, mutationIndex, entry, i);
                //indexRowAccumulator.add(pos++, null);
            } else {
                mainPutDecoder.seekForward(mutationDecoder, i);
            }
        }
        /*for (int i = mutationIndex.nextSetBit(0); i >= 0 && i <= indexedColumns.length(); i = mutationIndex.nextSetBit(i + 1)) {
            if (pkIndexColumns.get(i)) {
               //BitSet
            }
            else if (indexedColumns.get(i)) {
                if (isUniqueWithDuplicateNulls && !makeUniqueForDuplicateNulls) {
                    makeUniqueForDuplicateNulls = mainPutDecoder.nextIsNull(i);
                }
                ByteBuffer entry = mainPutDecoder.nextAsBuffer(mutationDecoder, i);
                if (descColumns.get(mainColToIndexPosMap[i]))
                    accumulate(indexKeyAccumulator, mutationIndex, getDescendingBuffer(entry), i);
                else
                    accumulate(indexKeyAccumulator, mutationIndex, entry, i);
                //accumulate(indexRowAccumulator, mutationIndex, entry, i);
            } else {
                mainPutDecoder.seekForward(mutationDecoder, i);
            }
        }*/
        //add the row location to the end of the index row
        indexRowAccumulator.add((int) translatedIndexedColumns.length(), ByteBuffer.wrap(Encoding.encodeBytesUnsorted(mutation.getRow())));
        // only make the call to check the accumulator if we have to -- if we haven't already determined
        makeUniqueForDuplicateNulls = (isUniqueWithDuplicateNulls &&
                (makeUniqueForDuplicateNulls || indexKeyAccumulator.getRemainingFields().cardinality() > 0));
        byte[] indexRowKey = getIndexRowKey(mutation.getRow(), (!isUnique || makeUniqueForDuplicateNulls));
        byte[] indexRowData = indexRowAccumulator.finish();
        LOG.error("key = " + BytesUtil.toHex(indexRowKey));
        LOG.error("value = " + BytesUtil.toHex(indexRowData));
        return new KVPair(indexRowKey, indexRowData, mutation.getType());
    }

    private void accumulate(EntryAccumulator accumulator, BitIndex index, ByteBuffer buffer, int position) {
        if (index.isScalarType(position))
            accumulator.addScalar(mainColToIndexPosMap[position], buffer);
        else if (index.isFloatType(position))
            accumulator.addFloat(mainColToIndexPosMap[position], buffer);
        else if (index.isDoubleType(position))
            accumulator.addDouble(mainColToIndexPosMap[position], buffer);
        else
            accumulator.add(mainColToIndexPosMap[position], buffer);
    }

    public byte[] getIndexRowKey(byte[] rowKey, boolean makeUniqueRowKey){
        if(makeUniqueRowKey)
            indexKeyAccumulator.add((int) translatedIndexedColumns.length(), ByteBuffer.wrap(rowKey));

        return indexKeyAccumulator.finish();
    }

    protected ByteBuffer getDescendingBuffer(ByteBuffer entry) {
        entry.mark();
        byte[] data = new byte[entry.remaining()];
        entry.get(data);
        entry.reset();
        for (int i = 0; i < data.length; i++) {
            data[i] ^= 0xff;
        }
        return ByteBuffer.wrap(data);
    }

    public EntryAccumulator getRowAccumulator() {
        if (indexRowAccumulator == null)
            indexRowAccumulator = new SparseEntryAccumulator(null, nonUniqueIndexedColumns, true);
        return indexRowAccumulator;
    }

    public EntryAccumulator getKeyAccumulator() {
        if (indexKeyAccumulator == null)
            indexKeyAccumulator = new SparseEntryAccumulator(null, isUnique ? translatedIndexedColumns : nonUniqueIndexedColumns, false);
        return indexKeyAccumulator;
    }

    public boolean isUnique() {
        return isUnique;
    }

    private class KeyData {
        private int offset;
        private int size;

        public KeyData(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }

        public int getOffset() {
            return offset;
        }

        public int getSize() {
            return size;
        }
    }
}
