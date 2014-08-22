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

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Builds an index table KVPair given a base table KVPair.
 *
 * Transform:
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
public class IndexTransformer {

    /* Index for which we are transforming may be unique or not */
    private final boolean isUnique;
    /* If true then we append srcRowKey to our emitted indexRowKey, even for unique indexes, for rows with nulls */
    private final boolean isUniqueWithDuplicateNulls;
    /* Source table column types */
    private final int[] columnTypes;
    /* Indices of the primary keys in source table */
    private final int[] srcPrimaryKeyIndices;
    /* Sort order or primary keys in src table */
    private final boolean[] srcKeyColumnSortOrder;
    /* For each column index in the src table either -1 or the position of that column in the index  */
    private final int[] indexKeyEncodingMap;
    /* Sort order for each column encoded in indexRowKey */
    private final boolean[] indexKeySortOrder;

    private final TypeProvider typeProvider;

    private MultiFieldDecoder srcKeyDecoder;
    private EntryDecoder srcValueDecoder;

    private ByteEntryAccumulator indexKeyAccumulator;
    private EntryEncoder indexValueEncoder;

    public IndexTransformer(boolean isUnique,
                            boolean isUniqueWithDuplicateNulls,
                            String tableVersion,
                            int[] srcPrimaryKeyIndices,
                            int[] columnTypes,
                            boolean[] srcKeyColumnSortOrder,
                            int[] indexKeyEncodingMap,
                            boolean[] indexKeySortOrder) {

        checkArgument(!isUniqueWithDuplicateNulls || isUnique, "isUniqueWithDuplicateNulls only for use with unique indexes");

        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.srcPrimaryKeyIndices = srcPrimaryKeyIndices;
        this.columnTypes = columnTypes;
        this.srcKeyColumnSortOrder = srcKeyColumnSortOrder;
        this.indexKeyEncodingMap = indexKeyEncodingMap;
        this.indexKeySortOrder = indexKeySortOrder;
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
        if (srcPrimaryKeyIndices != null) {
            //we have key columns to check
            MultiFieldDecoder keyDecoder = getSrcKeyDecoder();
            keyDecoder.set(mutation.getRow());
            for (int sourceKeyColumnPos : srcPrimaryKeyIndices) {
                int indexKeyPos = sourceKeyColumnPos < indexKeyEncodingMap.length ? indexKeyEncodingMap[sourceKeyColumnPos] : -1;
                if (indexKeyPos < 0) {
                    skip(keyDecoder, columnTypes[sourceKeyColumnPos]);
                } else {
                    int offset = keyDecoder.offset();
                    boolean isNull = skip(keyDecoder, columnTypes[sourceKeyColumnPos]);
                    hasNullKeyFields = isNull || hasNullKeyFields;
                    if (!isNull) {
                        int length = keyDecoder.offset() - offset - 1;
                        accumulate(keyAccumulator, indexKeyPos,
                                columnTypes[sourceKeyColumnPos],
                                srcKeyColumnSortOrder != null && srcKeyColumnSortOrder[sourceKeyColumnPos] != indexKeySortOrder[sourceKeyColumnPos],
                                keyDecoder.array(), offset, length);
                    }
                }
            }
        }

        /*
         * Handle non-null index columns from the source tables non-primary key columns.
         */
        EntryDecoder rowDecoder = getSrcValueDecoder();
        rowDecoder.set(mutation.getValue());
        BitIndex index = rowDecoder.getCurrentIndex();
        MultiFieldDecoder rowFieldDecoder = rowDecoder.getEntryDecoder();
        for (int i = index.nextSetBit(0); i >= 0; i = index.nextSetBit(i + 1)) {
            int keyColumnPos = i < indexKeyEncodingMap.length ? indexKeyEncodingMap[i] : -1;
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
                            !indexKeySortOrder[keyColumnPos],
                            rowFieldDecoder.array(), offset, length);
                }
            }
        }

        /*
         * Handle NULL index columns from the source tables non-primary key columns.
         */
        for (int srcColIndex = 0; srcColIndex < indexKeyEncodingMap.length; srcColIndex++) {
            /* position of the source column within the index encoding */
            int indexColumnPosition = indexKeyEncodingMap[srcColIndex];
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
        if (srcPrimaryKeyIndices != null) {
            for (int srcPrimaryKeyIndex : srcPrimaryKeyIndices) {
                if (srcPrimaryKeyIndex == sourceColumnIndex) {
                    return true;
                }
            }
        }
        return false;
    }

    public byte[] getIndexRowKey(byte[] rowLocation, boolean nonUnique) {
        byte[] data = indexKeyAccumulator.finish();
        if (nonUnique) {
            //append the row location to the end of the bytes
            byte[] newData = Arrays.copyOf(data, data.length + rowLocation.length + 1);
            System.arraycopy(rowLocation, 0, newData, data.length + 1, rowLocation.length);
            data = newData;
        }
        return data;
    }

    public EntryEncoder getRowEncoder() {
        if (indexValueEncoder == null) {
            BitSet nonNullFields = new BitSet();
            int highestSetPosition = 0;
            for (int keyColumn : indexKeyEncodingMap) {
                if (keyColumn > highestSetPosition)
                    highestSetPosition = keyColumn;
            }
            nonNullFields.set(highestSetPosition + 1);
            indexValueEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(), 1, nonNullFields,
                    BitSet.newInstance(), BitSet.newInstance(), BitSet.newInstance());
        }
        return indexValueEncoder;
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

    private MultiFieldDecoder getSrcKeyDecoder() {
        if (srcKeyDecoder == null)
            srcKeyDecoder = MultiFieldDecoder.create();
        return srcKeyDecoder;
    }

    public ByteEntryAccumulator getKeyAccumulator() {
        if (indexKeyAccumulator == null) {
            BitSet keyFields = BitSet.newInstance();
            for (int keyColumn : indexKeyEncodingMap) {
                if (keyColumn >= 0)
                    keyFields.set(keyColumn);
            }
            indexKeyAccumulator = new ByteEntryAccumulator(EntryPredicateFilter.emptyPredicate(), keyFields);
        }

        return indexKeyAccumulator;
    }

    private EntryDecoder getSrcValueDecoder() {
        if (srcValueDecoder == null)
            srcValueDecoder = new EntryDecoder();
        return srcValueDecoder;
    }

    public boolean isUnique() {
        return isUnique;
    }

}
