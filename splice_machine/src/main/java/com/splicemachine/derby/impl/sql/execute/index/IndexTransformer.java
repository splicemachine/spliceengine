package com.splicemachine.derby.impl.sql.execute.index;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.util.Arrays;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.storage.index.BitIndex;

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
                            int[] mainColToIndexPosMap,
                            BitSet descColumns,
                            BitSet indexedColumns) {

        checkArgument(!isUniqueWithDuplicateNulls || isUnique, "isUniqueWithDuplicateNulls only for use with unique indexes");

        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.srcPrimaryKeyIndices = srcPrimaryKeyIndices;
        this.columnTypes = columnTypes;
        this.srcKeyColumnSortOrder = srcKeyColumnSortOrder;
        this.typeProvider = VersionedSerializers.typesForVersion(tableVersion);

        boolean[] destKeySortOrder = new boolean[columnTypes.length];
        Arrays.fill(destKeySortOrder, true);
        for (int i = descColumns.nextSetBit(0); i >= 0; i = descColumns.nextSetBit(i + 1)) {
            destKeySortOrder[i] = false;
        }
        this.indexKeySortOrder = destKeySortOrder;

        int[] keyDecodingMap = new int[(int) indexedColumns.length()];
        Arrays.fill(keyDecodingMap, -1);
        for (int i = indexedColumns.nextSetBit(0); i >= 0; i = indexedColumns.nextSetBit(i + 1)) {
            keyDecodingMap[i] = mainColToIndexPosMap[i];
        }
        this.indexKeyEncodingMap = keyDecodingMap;
    }

    // For testing only
    IndexTransformer(boolean isUnique,
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

    /**
     * @return true if this is a unique index.
     */
    public boolean isUniqueIndex() {
        return isUnique;
    }

    /**
     * Create a KVPair that can be used to issue a delete on an index record associated with the given main
     * table mutation.
     * @param mutation the incoming modification. Its rowKey is used to get the the row in the base table
     *                 that will be updated. Once we have that, we can create a new KVPair of type
     *                 {@link com.splicemachine.hbase.KVPair.Type#DELETE DELETE} and call {@link #translate(KVPair)}
     *                 to translate it to the index's rowKey.
     * @param ctx the write context of the modification. Used to get transaction and region info.
     * @param indexedColumns the columns which are part of this index. Used to filter the Get.
     * @return An index row KVPair that can be used to delete the associated index row, or null if the mutated
     * row is not found (may have already been deleted).
     * @throws IOException for encoding/decoding problems.
     */
    public KVPair createIndexDelete(KVPair mutation, WriteContext ctx, BitSet indexedColumns) throws IOException {
        // do a Get() on all the indexed columns of the base table
        Get get = SpliceUtils.createGet(ctx.getTxn(), mutation.getRowKey());
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(),true);
        get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
        Result result = ctx.getRegion().get(get);
        if(result==null||result.isEmpty()){
            // we can't find the old row, may have been deleted already
            return null;
        }

        KeyValue resultValue = null;
        for(KeyValue value:result.raw()){
            if(CellUtil.matchingFamily(value, SpliceConstants.DEFAULT_FAMILY_BYTES)
                && CellUtil.matchingQualifier(value,SpliceConstants.PACKED_COLUMN_BYTES)){
                resultValue = value;
                break;
            }
        }
        if(resultValue==null){
            // we can't find the old row, may have been deleted already
            return null;
        }

        // transform the results into an index row (as if we were inserting it) but create a delete for it
        KVPair toTransform = new KVPair(get.getRow(),resultValue.getValue(), KVPair.Type.DELETE);
        return translate(toTransform);
    }

    /**
     * Translate the given base table record mutation into its associated, referencing index record.<br/>
     * Encapsulates the logic required to create an index record for a given base table record with
     * all the required discriminating and encoding rules (column is part of a PK, value is null, etc).
     * @param mutation KVPair containing the rowKey of the base table record for which we want to
     *                 translate to the associated index. This mutation should already have its requred
     *                 {@link com.splicemachine.hbase.KVPair.Type Type} set.
     * @return A KVPair representing the index record of the given base table mutation. This KVPair is
     * suitable for performing the required modification of the index record associated with this mutation.
     * @throws IOException for encoding/decoding problems.
     */
    public KVPair translate(KVPair mutation) throws IOException {
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
            keyDecoder.set(mutation.getRowKey());
            for (int sourceKeyColumnPos : srcPrimaryKeyIndices) {
                int indexKeyPos = sourceKeyColumnPos < indexKeyEncodingMap.length ? indexKeyEncodingMap[sourceKeyColumnPos] : -1;
                int offset = keyDecoder.offset();
                boolean isNull = skip(keyDecoder, columnTypes[sourceKeyColumnPos]);
                if(indexKeyPos>=0){
                    /*
                     * since primary keys have an implicit NOT NULL constraint here, we don't need to check for it,
                     * and isNull==true would represent a programmer error, rather than an actual state the
                     * system can be in.
                     */
                    assert !isNull: "Programmer error: Cannot update a primary key to a null value!";
                    int length = keyDecoder.offset() - offset - 1;
                    accumulate(keyAccumulator, indexKeyPos,
                            columnTypes[sourceKeyColumnPos],
                            srcKeyColumnSortOrder != null && srcKeyColumnSortOrder[sourceKeyColumnPos] != indexKeySortOrder[sourceKeyColumnPos],
                            keyDecoder.array(), offset, length);
                }
            }
        }

        /*
         * Handle non-null index columns from the source tables non-primary key columns.
         *
         * this will set indexed columns with values taken from the incoming mutation (rather than
         * backfilling them with existing values, which would occur elsewhere).
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
                int length;
                if (!isNull) {
                    length = rowFieldDecoder.offset() - offset - 1;
                    accumulate(keyAccumulator,
                            keyColumnPos,
                            columnTypes[i],
                            !indexKeySortOrder[keyColumnPos],
                            rowFieldDecoder.array(), offset, length);
                } else{
                    /*
                     * because the field is NULL and it's source is the incoming mutation, we
                     * still need to accumulate it. We must be careful, however, to accumulate the
                     * proper null value.
                     *
                     * In theory, we could use a sparse encoding here--just accumulate a length 0 entry,
                     * which will allow us to use a very short row key to determine nullity. However, that
                     * doesn't work correctly, because doubles and floats at the end of the index might decode
                     * the row key as a double, resulting in goofball answers.
                     *
                     * Instead, we must use the dense encoding approach here. That means that we must
                     * select the proper dense type based on columnTypes[i]. For most data types, this is still
                     * a length-0 array, but for floats and doubles it will put the proper type into place.
                     */
                    accumulateNull(keyAccumulator,
                            keyColumnPos,
                            columnTypes[i]);
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
        byte[] srcRowKey = Encoding.encodeBytesUnsorted(mutation.getRowKey());

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


    public boolean primaryKeyUpdateOnly(KVPair mutation, WriteContext ctx, BitSet indexedColumns) {
        // This gives us the non-primary-key columns that this mutation modifies.
        EntryDecoder newPutDecoder = new EntryDecoder();
        newPutDecoder.set(mutation.getValue());
        BitIndex updateIndex = newPutDecoder.getCurrentIndex();

        boolean primaryKeyIndexColumnUpdatedOnly = true;
        for (int i = updateIndex.nextSetBit(0); i >= 0; i = updateIndex.nextSetBit(i + 1)) {
            if (indexedColumns.get(i)) {
                primaryKeyIndexColumnUpdatedOnly = false;
                break;
            }
        }
        return primaryKeyIndexColumnUpdatedOnly;
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

    private byte[] getIndexRowKey(byte[] rowLocation, boolean nonUnique) {
        byte[] data = indexKeyAccumulator.finish();
        if (nonUnique) {
            //append the row location to the end of the bytes
            byte[] newData = Arrays.copyOf(data, data.length + rowLocation.length + 1);
            System.arraycopy(rowLocation, 0, newData, data.length + 1, rowLocation.length);
            data = newData;
        }
        assert data != null && data.length>0:"getIndexRowKey returned invalid data";
        return data;
    }

    private EntryEncoder getRowEncoder() {
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

    private void accumulateNull(EntryAccumulator keyAccumulator, int pos, int type){
        if (typeProvider.isScalar(type))
            keyAccumulator.addScalar(pos,HConstants.EMPTY_BYTE_ARRAY,0,0);
        else if (typeProvider.isDouble(type))
            keyAccumulator.addDouble(pos,Encoding.encodedNullDouble(),0,Encoding.encodedNullDoubleLength());
        else if (typeProvider.isFloat(type))
            keyAccumulator.addDouble(pos,Encoding.encodedNullFloat(),0,Encoding.encodedNullFloatLength());
        else
            keyAccumulator.add(pos,HConstants.EMPTY_BYTE_ARRAY,0,0);

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

    private ByteEntryAccumulator getKeyAccumulator() {
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
}
