package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;
import java.util.Arrays;

/**
 * Transform
 *
 * [srcRowKey, srcValue]  -> [indexRowKey, indexValue]
 *
 * Where
 *
 * FOR NON-UNIQUE: indexRowKey = [col1] + 0 + [col2] ... [colN] + encodeBytesUnsorted(srcRowKey)
 * FOR UNIQUE:     indexRowKey = [col1] + 0 + [col2] ... [colN]
 *
 * And
 *
 * indexValue = multiValueEncoding( encodeBytesUnsorted ( srcRowKey ))
 *
 * Where colN is an indexed column that may be encoded as part of srcRowKey or srcValue depending on if it is part of
 * a primary key in the source table.
 *
 * @author Scott Fines
 *         Date: 4/17/14
 */
public class IndexTransformer2 {

    private ByteEntryAccumulator destKeyAccumulator;
    private EntryEncoder destRowEncoder;

    private final boolean isUnique;
    private final boolean isUniqueWithDuplicateNulls;

    private int[] sourceKeyColumnEncodingOrder;
    private int[] columnTypes;
    private boolean[] sourceKeyColumnSortOrder;

    private int[] destKeyEncodingMap;
    private boolean[] destKeyColumnSortOrder;

    private EntryDecoder rowDecoder;
    private MultiFieldDecoder keyDecoder;

    private TypeProvider typeProvider;

    public IndexTransformer2(boolean isUnique,
                             boolean isUniqueWithDuplicateNulls,
                             String tableVersion,
                             int[] sourceKeyColumnEncodingOrder,
                             int[] columnTypes,
                             boolean[] sourceKeyColumnSortOrder,
                             int[] destKeyEncodingMap,
                             boolean[] destKeyColumnSortOrder) {
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.sourceKeyColumnEncodingOrder = sourceKeyColumnEncodingOrder;
        this.columnTypes = columnTypes;
        this.sourceKeyColumnSortOrder = sourceKeyColumnSortOrder;
        this.destKeyEncodingMap = destKeyEncodingMap;
        this.destKeyColumnSortOrder = destKeyColumnSortOrder;
        this.typeProvider = VersionedSerializers.typesForVersion(tableVersion);
    }

    public KVPair translate(KVPair mutation) throws IOException, StandardException {
        if (mutation == null) {
            return null;
        }

        EntryAccumulator keyAccumulator = getKeyAccumulator();
        keyAccumulator.reset();
        boolean hasNullKeyFields = false;

        /*
         * Handle index columns from the source table's primary key.
         */
        if (sourceKeyColumnEncodingOrder != null) {
            //we have key columns to check
            MultiFieldDecoder keyDecoder = getKeyDecoder();
            keyDecoder.set(mutation.getRow());
            for (int sourceKeyColumnPos : sourceKeyColumnEncodingOrder) {
                int destKeyPos = sourceKeyColumnPos < destKeyEncodingMap.length ? destKeyEncodingMap[sourceKeyColumnPos] : -1;
                if (destKeyPos < 0) {
                    skip(keyDecoder, columnTypes[sourceKeyColumnPos]);
                } else {
                    int offset = keyDecoder.offset();
                    boolean isNull = skip(keyDecoder, columnTypes[sourceKeyColumnPos]);
                    hasNullKeyFields = isNull || hasNullKeyFields;
                    if (!isNull) {
                        int length = keyDecoder.offset() - offset - 1;
                        accumulate(keyAccumulator, destKeyPos,
                                columnTypes[sourceKeyColumnPos],
                                sourceKeyColumnSortOrder != null && sourceKeyColumnSortOrder[sourceKeyColumnPos] != destKeyColumnSortOrder[sourceKeyColumnPos],
                                keyDecoder.array(), offset, length);
                    }
                }
            }
        }

        /*
         * Handle non-null index columns from the source tables non-primary key columns.
         */
        EntryDecoder rowDecoder = getRowDecoder();
        rowDecoder.set(mutation.getValue());
        BitIndex index = rowDecoder.getCurrentIndex();
        MultiFieldDecoder rowFieldDecoder = rowDecoder.getEntryDecoder();
        for (int i = index.nextSetBit(0); i >= 0; i = index.nextSetBit(i + 1)) {
            int keyColumnPos = i < destKeyEncodingMap.length ? destKeyEncodingMap[i] : -1;
            if (keyColumnPos < 0) {
                rowDecoder.seekForward(rowFieldDecoder, i);
            } else {
                int offset = rowFieldDecoder.offset();
                boolean isNull = rowDecoder.seekForward(rowFieldDecoder, i);
                hasNullKeyFields = isNull || hasNullKeyFields;
                if (!isNull) {
                    int length = rowFieldDecoder.offset() - offset - 1;
                    accumulate(keyAccumulator,
                            keyColumnPos,
                            columnTypes[i],
                            !destKeyColumnSortOrder[keyColumnPos],
                            rowFieldDecoder.array(), offset, length);
                }
            }
        }

        /*
         * Handle NULL index columns from the source tables non-primary key columns.
         */
        for (int srcColIndex = 0; srcColIndex < destKeyEncodingMap.length; srcColIndex++) {
            /* position of the source column within the index encoding */
            int indexColumnPosition = destKeyEncodingMap[srcColIndex];
            if (!isSourceColumnPrimaryKey(srcColIndex) && indexColumnPosition >= 0 && !index.isSet(srcColIndex)) {
                hasNullKeyFields = true;
                keyAccumulator.add(indexColumnPosition, new byte[]{}, 0, 0);
            }
        }

        //add the row key to the end of the index key
        byte[] srcRowKey = Encoding.encodeBytesUnsorted(mutation.getRow());

        EntryEncoder rowEncoder = getRowEncoder();
        MultiFieldEncoder entryEncoder = rowEncoder.getEntryEncoder();
        entryEncoder.reset();
        entryEncoder.setRawBytes(srcRowKey);
        byte[] indexValue = rowEncoder.encode();

        byte[] indexRowKey;
        if (isUnique) {
            boolean nonUnique = isUniqueWithDuplicateNulls && (hasNullKeyFields || !keyAccumulator.isFinished());
            indexRowKey = getIndexRowKey(srcRowKey, nonUnique);
        } else
            indexRowKey = getIndexRowKey(srcRowKey, true);

        return new KVPair(indexRowKey, indexValue, mutation.getType());
    }

    private boolean isSourceColumnPrimaryKey(int sourceColumnIndex) {
        if (sourceKeyColumnEncodingOrder != null) {
            for (int srcPrimaryKeyIndex : sourceKeyColumnEncodingOrder) {
                if (srcPrimaryKeyIndex == sourceColumnIndex) {
                    return true;
                }
            }
        }
        return false;
    }

    public byte[] getIndexRowKey(byte[] rowLocation, boolean nonUnique) {
        byte[] data = destKeyAccumulator.finish();
        if (nonUnique) {
            //append the row location to the end of the bytes
            byte[] newData = Arrays.copyOf(data, data.length + rowLocation.length + 1);
            System.arraycopy(rowLocation, 0, newData, data.length + 1, rowLocation.length);
            data = newData;
        }
        return data;
    }

    public EntryEncoder getRowEncoder() {
        if (destRowEncoder == null) {
            BitSet nonNullFields = new BitSet();
            int highestSetPosition = 0;
            for (int keyColumn : destKeyEncodingMap) {
                if (keyColumn > highestSetPosition)
                    highestSetPosition = keyColumn;
            }
            nonNullFields.set(highestSetPosition + 1);
            destRowEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(), 1, nonNullFields,
                    BitSet.newInstance(), BitSet.newInstance(), BitSet.newInstance());
        }
        return destRowEncoder;
    }

    private void accumulate(EntryAccumulator keyAccumulator, int pos,
                            int type,
                            boolean reverseOrder,
                            byte[] array, int offset, int length) {
        byte[] data = array;
        int off = offset;
        if (reverseOrder) {
            //TODO -sf- could we cache these byte[] somehow?
            data = new byte[length];
            System.arraycopy(array, offset, data, 0, length);
            for (int i = 0; i < data.length; i++) {
                data[i] ^= 0xff;
            }
            off = 0;
        }
        if (typeProvider.isScalar(type))
            keyAccumulator.addScalar(pos, data, off, length);
        else if (typeProvider.isDouble(type))
            keyAccumulator.addDouble(pos, data, off, length);
        else if (typeProvider.isFloat(type))
            keyAccumulator.addFloat(pos, data, off, length);
        else
            keyAccumulator.add(pos, data, off, length);

    }

    private boolean skip(MultiFieldDecoder keyDecoder, int sourceKeyColumnType) {
        boolean isNull;
        if (typeProvider.isScalar(sourceKeyColumnType)) {
            isNull = keyDecoder.nextIsNull();
            keyDecoder.skipLong();
        } else if (typeProvider.isDouble(sourceKeyColumnType)) {
            isNull = keyDecoder.nextIsNullDouble();
            keyDecoder.skipDouble();
        } else if (typeProvider.isFloat(sourceKeyColumnType)) {
            isNull = keyDecoder.nextIsNullFloat();
            keyDecoder.skipFloat();
        } else {
            isNull = keyDecoder.nextIsNull();
            keyDecoder.skip();
        }
        return isNull;
    }

    private MultiFieldDecoder getKeyDecoder() {
        if (keyDecoder == null)
            keyDecoder = MultiFieldDecoder.create();
        return keyDecoder;
    }

    public ByteEntryAccumulator getKeyAccumulator() {
        if (destKeyAccumulator == null) {
            BitSet keyFields = BitSet.newInstance();
            for (int keyColumn : destKeyEncodingMap) {
                if (keyColumn >= 0)
                    keyFields.set(keyColumn);
            }
            destKeyAccumulator = new ByteEntryAccumulator(EntryPredicateFilter.emptyPredicate(), keyFields);
        }

        return destKeyAccumulator;
    }

    public EntryDecoder getRowDecoder() {
        if (rowDecoder == null)
            rowDecoder = new EntryDecoder();
        return rowDecoder;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public static class KeyData {
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
