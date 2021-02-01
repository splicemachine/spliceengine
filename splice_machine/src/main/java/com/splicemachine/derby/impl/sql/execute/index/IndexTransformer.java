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

package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.google.protobuf.ByteString;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.GeneratedClass;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.sql.execute.BaseExecutableIndexExpression;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.ExecRowAccumulator;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.SerializationUtils;
import splice.com.google.common.primitives.Ints;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static splice.com.google.common.base.Preconditions.checkArgument;

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
@NotThreadSafe
public class IndexTransformer {
    private TypeProvider typeProvider;
    private MultiFieldDecoder srcKeyDecoder;
    private EntryDecoder srcValueDecoder;
    private ByteEntryAccumulator indexKeyAccumulator;
    private EntryEncoder indexValueEncoder;
    private DDLMessage.Index index;
    private int [] mainColToIndexStoragePosMap;
    private BitSet indexedStorageCols;
    private byte[] indexConglomBytes;
    private Map<Integer, Integer>  formatIdsMap;
    private int[] columnOrderingStoragePos;
    private DescriptorSerializer[] indexRowSerializers;
    private DescriptorSerializer[] baseRowSerializers;
    private boolean excludeNulls;
    private boolean excludeDefaultValues;
    private DataValueDescriptor defaultValue;
    private ValueRow defaultValuesExecRow;
    private ExecRowAccumulator execRowAccumulator;
    private transient DataGet baseGet = null;
    private transient DataResult baseResult = null;
    private boolean ignore;
    protected EntryDataHash entryEncoder;
    private final int numIndexExprs;
    private final BaseExecutableIndexExpression[] executableExprs;
    private final LanguageConnectionContext lcc;
    private final String version;

    public IndexTransformer(DDLMessage.TentativeIndex tentativeIndex) throws StandardException {
        index = tentativeIndex.getIndex();
        DDLMessage.Table table = tentativeIndex.getTable();
        checkArgument(!index.getUnique() || index.getUniqueWithDuplicateNulls(), "isUniqueWithDuplicateNulls only for use with unique indexes");
        excludeNulls = index.getExcludeNulls();
        version = table.getTableVersion();
        excludeDefaultValues = index.getExcludeDefaults();
        this.typeProvider = VersionedSerializers.typesForVersion(version);
        indexedStorageCols = DDLUtils.asBitSet(Ints.toArray(index.getIndexColsToMainStorageColMapList()));
        mainColToIndexStoragePosMap = DDLUtils.invert(Ints.toArray(index.getIndexColsToMainStorageColMapList()));
        indexConglomBytes = DDLUtils.getIndexConglomBytes(index.getConglomerate());
        if (index.hasDefaultValues() && excludeDefaultValues) {
            defaultValue = (DataValueDescriptor) SerializationUtils.deserialize(index.getDefaultValues().toByteArray());
            defaultValuesExecRow = new ValueRow(new DataValueDescriptor[]{defaultValue.cloneValue(true)});
        }
        numIndexExprs = index.getNumExprs();
        executableExprs = new BaseExecutableIndexExpression[numIndexExprs];
        columnOrderingStoragePos = Ints.toArray(table.getColumnOrderingList().stream().map(i -> table.getFormatStorageIds(i) - 1).collect(Collectors.toList()));
        formatIdsMap = new HashMap<>();
        if (numIndexExprs > 0) {
            assert index.getIndexColumnFormatIdsCount() == numIndexExprs;
            for (int i = 0; i < index.getIndexColumnFormatIdsCount(); ++i) {
                formatIdsMap.put(i, index.getIndexColumnFormatIds(i));
            }
            lcc = getLcc(tentativeIndex);
        } else {
            assert table.getFormatIdsCount() == table.getFormatStorageIdsCount();
            for (int i = 0; i < table.getFormatIdsCount(); ++i) {
                formatIdsMap.put(table.getFormatStorageIds(i), table.getFormatIds(i));
            }
            lcc = null;
        }
    }

    /**
     * @return true if this is a unique index.
     */
    public boolean isUniqueIndex() {
        return index.getUnique();
    }

    public BitSet gitIndexedCols() {
        return indexedStorageCols;
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public byte[] getIndexConglomBytes() {
        return indexConglomBytes;
    }
    /**
     * Create a KVPair that can be used to issue a delete on an index record associated with the given main
     * table mutation.
     * @param mutation the incoming modification. Its rowKey is used to get the the row in the base table
     *                 that will be updated. Once we have that, we can create a new KVPair of type
     *                 {@link KVPair.Type#DELETE DELETE} and call {@link #translate(KVPair)}
     *                 to translate it to the index's rowKey.
     * @param ctx the write context of the modification. Used to get transaction and region info.
     * @param indexedColumns the columns which are part of this index. Used to filter the Get.
     * @return An index row KVPair that can be used to delete the associated index row, or null if the mutated
     * row is not found (may have already been deleted).
     * @throws IOException for encoding/decoding problems.
     */
    public KVPair createIndexDelete(KVPair mutation, WriteContext ctx, BitSet indexedColumns)
            throws IOException, StandardException
    {
        // do a Get() on all the indexed columns of the base table
        DataResult result =fetchBaseRow(mutation,ctx,indexedColumns);
        if(result==null || result.isEmpty()){
            // we can't find the old row, may have been deleted already
            return null;
        }

        DataCell resultValue = result.userData();
        if(resultValue==null){
            // we can't find the old row, may have been deleted already
            return null;
        }

        // transform the results into an index row (as if we were inserting it) but create a delete for it

        KVPair toTransform = new KVPair(
                resultValue.keyArray(),resultValue.keyOffset(),resultValue.keyLength(),
                resultValue.valueArray(),resultValue.valueOffset(),resultValue.valueLength(),KVPair.Type.DELETE);
        return translate(toTransform);
    }

    public KVPair encodeSystemTableIndex(ExecRow execRow) throws StandardException, IOException {

        boolean isUnique = index.getUnique();
        boolean[] order = new boolean[isUnique ? index.getDescColumnsCount() : index.getDescColumnsCount() + 1];
        for (int i = 0; i < order.length; ++i) {
            if (i < index.getDescColumnsCount()) {
                order[i] = !index.getDescColumns(i);
            }
            else {
                order[i] = true;
            }
        }
        DataValueDescriptor[] dvds = execRow.getRowArray();
        if (index.getUnique()) {
            dvds = new DataValueDescriptor[execRow.nColumns()-1];
            System.arraycopy(execRow.getRowArray(),0, dvds,0, dvds.length);
        }
        byte[] key = DerbyBytesUtil.generateIndexKey(dvds, order,version,false);

        if(entryEncoder==null){
            int[] validCols= EngineUtils.bitSetToMap(null);
            DescriptorSerializer[] serializers=VersionedSerializers.forVersion(version,true).getSerializers(execRow);
            entryEncoder=new EntryDataHash(validCols,null,serializers);
        }
        ValueRow rowToEncode=new ValueRow(execRow.getRowArray().length);
        rowToEncode.setRowArray(execRow.getRowArray());
        entryEncoder.setRow(rowToEncode);
        byte[] value =entryEncoder.encode();
        return new KVPair(key, value);
    }



    public KVPair writeDirectIndex(ExecRow execRow, int[] storageMapping) throws IOException, StandardException {
        assert execRow != null: "ExecRow passed in is null";
        getIndexRowSerializers(execRow);
        EntryAccumulator keyAccumulator = getKeyAccumulator();
        keyAccumulator.reset();
        ignore = false;
        boolean hasNullKeyFields = false;
        for (int i = 0; i< execRow.nColumns();i++) {
            if (execRow.getColumn(i+1) == null || execRow.getColumn(i+1).isNull()) {
                if (i == 0 && excludeNulls) // Pass along null for exclusion...
                    return null;
                hasNullKeyFields = true;
                accumulateNull(keyAccumulator,
                    i,
                    formatIdsMap.get(storageMapping[i]));
            } else {
                byte[] data = indexRowSerializers[i].encodeDirect(execRow.getColumn(i+1),false);
                accumulate(keyAccumulator,
                    i, formatIdsMap.get(storageMapping[i]),
                    index.getDescColumns(i),
                    data, 0, data.length);
            }
        }

        if (ignore)
            return null;

        //add the row key to the end of the index key
        byte[] srcRowKey = Encoding.encodeBytesUnsorted(execRow.getKey());

        return getKvPair(keyAccumulator, hasNullKeyFields, srcRowKey, KVPair.Type.INSERT);
    }

    private KVPair getKvPair(EntryAccumulator keyAccumulator, boolean hasNullKeyFields, byte[] srcRowKey, KVPair.Type type) {
        EntryEncoder rowEncoder = getRowEncoder();
        MultiFieldEncoder entryEncoder = rowEncoder.getEntryEncoder();
        entryEncoder.reset();
        entryEncoder.setRawBytes(srcRowKey);
        byte[] indexValue = rowEncoder.encode();
        byte[] indexRowKey;
        if (index.getUnique()) {
            boolean nonUnique = index.getUniqueWithDuplicateNulls() && (hasNullKeyFields || !keyAccumulator.isFinished());
            indexRowKey = getIndexRowKey(srcRowKey, nonUnique);
        } else
            indexRowKey = getIndexRowKey(srcRowKey, true);
        return new KVPair(indexRowKey, indexValue, type);
    }


    /**
     * Translate the given base table record mutation into its associated, referencing index record.<br/>
     * Encapsulates the logic required to create an index record for a given base table record with
     * all the required discriminating and encoding rules (column is part of a PK, value is null, etc).
     * @param mutation KVPair containing the rowKey of the base table record for which we want to
     *                 translate to the associated index. This mutation should already have its required
     *                 {@link KVPair.Type Type} set.
     * @return A KVPair representing the index record of the given base table mutation. This KVPair is
     * suitable for performing the required modification of the index record associated with this mutation.
     * @throws IOException for encoding/decoding problems.
     */
    public KVPair translate(KVPair mutation) throws IOException, StandardException {
        if (mutation == null) {
            return null;
        }

        EntryAccumulator keyAccumulator = getKeyAccumulator();
        keyAccumulator.reset();
        boolean hasNullKeyFields = false;

        // value decoder
        EntryDecoder rowDecoder = getSrcValueDecoder();
        rowDecoder.set(mutation.getValue());
        BitIndex bitIndex = rowDecoder.getCurrentIndex();
        int colCount = mainColToIndexStoragePosMap.length;
        ExecRow decodedRow = new ValueRow(colCount + 1);

        if (baseRowSerializers == null)
            baseRowSerializers = new DescriptorSerializer[decodedRow.nColumns()];

        ignore = false;

        /*
         * Handle index columns from the source table's primary key.
         */
        if (columnOrderingStoragePos.length>0) {
            //we have key columns to check
            MultiFieldDecoder keyDecoder = getSrcKeyDecoder();
            keyDecoder.set(mutation.getRowKey());
            for(int i=0, cnt=0;i<columnOrderingStoragePos.length;i++, cnt++){
                int pkPos = columnOrderingStoragePos[i];
                int formatId = formatIdsMap.get(pkPos);
                int offset = keyDecoder.offset();
                boolean isNull = skip(keyDecoder, formatId);
                int length = keyDecoder.offset() - offset - 1;

                if (!indexedStorageCols.get(pkPos)) continue;
                if (numIndexExprs <= 0) {
                    int indexKeyPos = pkPos < mainColToIndexStoragePosMap.length ?
                            mainColToIndexStoragePosMap[pkPos] : -1;
                    if (indexKeyPos >= 0) {
                        /*
                         * since primary keys have an implicit NOT NULL constraint here, we don't need to check for it,
                         * and isNull==true would represent a programmer error, rather than an actual state the
                         * system can be in.
                         */
                        assert !isNull : "Programmer error: Cannot update a primary key to a null value!";
                        /*
                         * A note about sort order:
                         *
                         * We are in the primary key section, which means that the element is ordered in
                         * ASCENDING order. In an ideal world, that wouldn't matter.
                         */
                        accumulate(keyAccumulator, indexKeyPos,
                                formatId,
                                index.getDescColumns(cnt),
                                keyDecoder.array(), offset, length);
                    }
                } else {
                    decodedRow.setColumn(pkPos + 1,
                            DecodeBaseRowColumn(keyDecoder, offset, length, pkPos, formatId));
                }
            }
        }

        /*
         * Handle non-null index columns from the source tables non-primary key columns.
         *
         * this will set indexed columns with values taken from the incoming mutation (rather than
         * backfilling them with existing values, which would occur elsewhere).
         */
        MultiFieldDecoder rowFieldDecoder = rowDecoder.getEntryDecoder();
        for (int cnt = 0, i = bitIndex.nextSetBit(0); i >= 0; i = bitIndex.nextSetBit(i + 1), cnt++) {
            if(!indexedStorageCols.get(i)) {
                //skip non-indexed columns
                rowDecoder.seekForward(rowFieldDecoder,i);
                continue;
            }

            int formatId = formatIdsMap.get(cnt);
            int offset = rowFieldDecoder.offset();
            boolean isNull = rowDecoder.seekForward(rowFieldDecoder, i);
            int length = rowFieldDecoder.offset() - offset - 1;
            if (numIndexExprs <= 0) {
                int keyColumnPos = i < colCount ?
                        mainColToIndexStoragePosMap[i] : -1;
                if (keyColumnPos < 0) {
                    rowDecoder.seekForward(rowFieldDecoder, i);
                } else {
                    hasNullKeyFields = isNull || hasNullKeyFields;
                    if (!isNull) {
                        accumulate(keyAccumulator,
                                keyColumnPos,
                                formatId,
                                index.getDescColumns(keyColumnPos),
                                rowFieldDecoder.array(), offset, length);
                    } else {
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
                                formatId);
                    }
                }
            } else {
                decodedRow.setColumn(i + 1, DecodeBaseRowColumn(rowFieldDecoder, offset, length, i, formatId));
            }
        }

        /*
         * Handle NULL index columns from the source tables non-primary key columns.
         * NULL columns are not set in bitIndex.
         */
        for (int srcColIndex = 0; srcColIndex < colCount; srcColIndex++) {
            /* position of the source column within the index encoding */
            int indexColumnPosition = mainColToIndexStoragePosMap[srcColIndex];
            if (!isSourceColumnPrimaryKey(srcColIndex) && indexColumnPosition >= 0 && !bitIndex.isSet(srcColIndex)) {
                if (numIndexExprs > 0) {
                    // In case of an expression-based index, indexColumnPosition >= 0 only means base column at
                    // srcColIndex is referenced in one of the index expressions. Actual value (1, 2, or 5) of
                    // indexColumnPosition is meaningless and should not be used in any place.
                    int formatId = formatIdsMap.get(indexColumnPosition);
                    DataValueDescriptor dvd = DataValueFactoryImpl.getNullDVDWithUCS_BASICcollation(formatId);
                    decodedRow.setColumn(srcColIndex + 1, dvd);
                    // No need to set ignore for excludeNulls here, will be handled by writeDirectIndex later.
                } else {
                    if (excludeNulls && indexColumnPosition == 0)
                        ignore = true;
                    keyAccumulator.add(indexColumnPosition, new byte[]{}, 0, 0);
                }
                hasNullKeyFields = true;
            }
        }

        if (ignore) {
            return null;
        }

        if (numIndexExprs > 0) {
            ExecRow indexRow = new ValueRow(numIndexExprs);
            for (int i = 0; i < numIndexExprs; i++) {
                BaseExecutableIndexExpression execExpr = getExecutableIndexExpression(i);
                execExpr.runExpression(decodedRow, indexRow);
            }
            indexRow.setKey(mutation.getRowKey());
            int[] mapping = new int[numIndexExprs];
            for(int i=0; i < numIndexExprs; i++) {
                mapping[i] = i;
            }
            KVPair kv = writeDirectIndex(indexRow, mapping);
            if (kv == null) {
                return null;
            }
            kv.setType(mutation.getType());
            return kv;
        }

        //add the row key to the end of the index key
        byte[] srcRowKey = Encoding.encodeBytesUnsorted(mutation.getRowKey());

        return getKvPair(keyAccumulator, hasNullKeyFields, srcRowKey, mutation.getType());
    }

    /**
     * Do we need to update the index, i.e. did any of the values change?
     *
     * @param mutation
     * @param indexedColumns
     * @return
     */
    public boolean areIndexKeysModified(KVPair mutation, BitSet indexedColumns) {
        EntryDecoder newPutDecoder = new EntryDecoder();
        newPutDecoder.set(mutation.getValue());
        BitIndex updateIndex = newPutDecoder.getCurrentIndex();
        for (int i = updateIndex.nextSetBit(0); i >= 0; i = updateIndex.nextSetBit(i + 1)) {
            if (indexedColumns.get(i))
                return true;
        }
        return false;
    }

    private boolean isSourceColumnPrimaryKey(int colPos) {
        return Arrays.stream(columnOrderingStoragePos).anyMatch(i -> i == colPos);
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
            for (int indexKeyStorageColumn : mainColToIndexStoragePosMap) {
                if (indexKeyStorageColumn > highestSetPosition)
                    highestSetPosition = indexKeyStorageColumn;
            }
            nonNullFields.set(highestSetPosition + 1);
            indexValueEncoder = EntryEncoder.create(SpliceKryoRegistry.getInstance(), 1, nonNullFields,
                    new BitSet(), new BitSet(), new BitSet());
        }
        return indexValueEncoder;
    }
    /**
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
    private void accumulateNull(EntryAccumulator keyAccumulator, int pos, int type){
        if (excludeNulls && pos == 0) // Exclude Nulls
            ignore = true;
        if (typeProvider.isScalar(type))
            keyAccumulator.addScalar(pos,SIConstants.EMPTY_BYTE_ARRAY,0,0);
        else if (typeProvider.isDouble(type))
            keyAccumulator.addDouble(pos,Encoding.encodedNullDouble(),0,Encoding.encodedNullDoubleLength());
        else if (typeProvider.isFloat(type))
            keyAccumulator.addDouble(pos,Encoding.encodedNullFloat(),0,Encoding.encodedNullFloatLength());
        else
            keyAccumulator.add(pos,SIConstants.EMPTY_BYTE_ARRAY,0,0);

    }

    private void accumulate(EntryAccumulator keyAccumulator, int storagePos,
                            int type,
                            boolean reverseOrder,
                            byte[] array, int offset, int length) throws IOException {
        byte[] data = array;
        int off = offset;
        if (excludeDefaultValues && storagePos == 0 && defaultValue != null) { // Exclude Default Values
            ExecRowAccumulator era =  getExecRowAccumulator();
            era.reset();
            if (typeProvider.isScalar(type))
                era.addScalar(storagePos, data, off, length);
            else if (typeProvider.isDouble(type))
                era.addDouble(storagePos, data, off, length);
            else if (typeProvider.isFloat(type))
                era.addFloat(storagePos, data, off, length);
            else
                era.add(storagePos, data, off, length);
            try {
                if (!defaultValue.isNull() &&
                        defaultValue.equals(defaultValue,defaultValuesExecRow.getColumn(1)).getBoolean()) {
                    ignore = true;
                } } catch (StandardException se) {
                throw new IOException(se);
            }

        }


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
            keyAccumulator.addScalar(storagePos, data, off, length);
        else if (typeProvider.isDouble(type))
            keyAccumulator.addDouble(storagePos, data, off, length);
        else if (typeProvider.isFloat(type))
            keyAccumulator.addFloat(storagePos, data, off, length);
        else
            keyAccumulator.add(storagePos, data, off, length);
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
            BitSet keyFields = new BitSet();
            for (int keyColumn : mainColToIndexStoragePosMap) {
                if (keyColumn >= 0)
                    keyFields.set(keyColumn);
            }
            indexKeyAccumulator = new ByteEntryAccumulator(EntryPredicateFilter.emptyPredicate(), keyFields);
        }

        return indexKeyAccumulator;
    }

    private ExecRowAccumulator getExecRowAccumulator() {
        if (execRowAccumulator == null) {
            execRowAccumulator = ExecRowAccumulator.newAccumulator(EntryPredicateFilter.emptyPredicate(), false,
                                                                   defaultValuesExecRow, new int[]{0}, version, mainColToIndexStoragePosMap);
        }
        return execRowAccumulator;
    }

    private EntryDecoder getSrcValueDecoder() {
        if (srcValueDecoder == null)
            srcValueDecoder = new EntryDecoder();
        return srcValueDecoder;
    }

    private DescriptorSerializer[] getIndexRowSerializers(ExecRow execRow) {
        if (indexRowSerializers ==null)
            indexRowSerializers = VersionedSerializers.forVersion(version, true).getSerializers(execRow);
        return indexRowSerializers;
    }


    private DataResult fetchBaseRow(KVPair mutation,WriteContext ctx,BitSet indexedColumns) throws IOException {
        baseGet = SIDriver.driver().getOperationFactory().newDataGet(ctx.getTxn(), mutation.getRowKey(), baseGet);

        EntryPredicateFilter epf;
        if (indexedColumns != null && !indexedColumns.isEmpty()) {
            epf = new EntryPredicateFilter(indexedColumns);
        } else {
            epf = EntryPredicateFilter.emptyPredicate();
        }

        TransactionalRegion region = ctx.txnRegion();
        TxnFilter txnFilter = region.packedFilter(ctx.getTxn(), epf, false);
        baseGet.setFilter(txnFilter);
        baseResult = ctx.getRegion().get(baseGet, baseResult);
        return baseResult;
    }

    public int getNumIndexExprs() { return numIndexExprs; }

    public BaseExecutableIndexExpression getExecutableIndexExpression(int indexColumnPosition)
            throws StandardException
    {
        assert numIndexExprs == index.getBytecodeExprsCount() &&
                numIndexExprs == index.getGeneratedClassNamesCount();
        if (numIndexExprs <= 0 || indexColumnPosition < 0 || indexColumnPosition >= numIndexExprs)
            return null;

        assert numIndexExprs == executableExprs.length;
        if (executableExprs[indexColumnPosition] != null)
            return executableExprs[indexColumnPosition];

        ByteString bytes = index.getBytecodeExprs(indexColumnPosition);
        ByteArray bytecode = new ByteArray(bytes.toByteArray());
        String className = index.getGeneratedClassNames(indexColumnPosition);

        assert lcc != null;
        ClassFactory classFactory = lcc.getLanguageConnectionFactory().getClassFactory();
        GeneratedClass gc = classFactory.loadGeneratedClass(className, bytecode);
        executableExprs[indexColumnPosition] = (BaseExecutableIndexExpression) gc.newInstance(lcc);
        return executableExprs[indexColumnPosition];
    }

    public int getMaxBaseColumnPosition() {
        return Collections.max(index.getIndexColsToMainColMapList());
    }

    private LanguageConnectionContext getLcc(DDLMessage.TentativeIndex tentativeIndex) throws StandardException {
        /* TODO(DB-10485) SpliceTransactionResourceImpl is closed by the time we use the LCC. It works in our small
         *  use-case here, but could lead to complications if a future commit tries to access the context manager from
         *  that LCC. We should either pass LCC from a caller function, or get ClassFactory directly.
         */
        TxnView txn = DDLUtils.getLazyTransaction(tentativeIndex.getTxnId());
        try (SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl()) {
            transactionResource.marshallTransaction(txn);
            return transactionResource.getLcc();
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private DataValueDescriptor DecodeBaseRowColumn(MultiFieldDecoder decoder, int offset, int length,
                                                    int srcColIndex, int formatId)
            throws StandardException
    {
        if (baseRowSerializers[srcColIndex] == null) {
            baseRowSerializers[srcColIndex] =
                    VersionedSerializers.forVersion(version, true).getSerializer(formatId);
        }
        DataValueDescriptor dvd = DataValueFactoryImpl.getNullDVDWithUCS_BASICcollation(formatId);
        baseRowSerializers[srcColIndex].decodeDirect(dvd, decoder.array(), offset, length, false);
        return dvd;
    }
}
