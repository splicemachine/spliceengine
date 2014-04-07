package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import java.io.IOException;
/**
 * Class responsible for transforming an incoming main table row
 * into an index row
 *
 * @author Scott Fines
 *         Created on: 8/23/13
 */
public class IndexTransformer {
    private final KryoPool kryoPool;
    private final BitSet indexedColumns;
    private final BitSet nonUniqueIndexedColumns;
    private final BitSet translatedIndexedColumns;
    private final boolean isUnique;
    private final boolean isUniqueWithDuplicateNulls;
    private final int[] mainColToIndexPosMap;
    private final BitSet descColumns;
    private ByteEntryAccumulator indexKeyAccumulator;
    private ByteEntryAccumulator indexRowAccumulator;
    private EntryDecoder mainPutDecoder;
    private int[] columnOrdering;
    private int[] format_ids;
    private KeyData[] pkIndex;
    private DataValueDescriptor[] kdvds;
    private DataValueDescriptor[] idvds;
    private MultiFieldDecoder keyDecoder;
    private BitSet pkColumns;
    private BitSet nonPkColumns;
    private BitSet pkIndexColumns;
    private int[] reverseColumnOrdering;

    private static final Logger LOG = Logger.getLogger(IndexTransformer.class);
    public IndexTransformer(BitSet indexedColumns,
                            BitSet translatedIndexedColumns,
                            BitSet nonUniqueIndexedColumns,
                            BitSet descColumns,
                            int[] mainColToIndexPosMap,
                            boolean isUnique,
                            boolean isUniqueWithDuplicateNulls,
                            KryoPool kryoPool) {
        this.indexedColumns = indexedColumns;
        this.translatedIndexedColumns = translatedIndexedColumns;
        this.nonUniqueIndexedColumns = nonUniqueIndexedColumns;
        this.descColumns = descColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.kryoPool = kryoPool;
    }

    public IndexTransformer(BitSet indexedColumns,
                            BitSet translatedIndexedColumns,
                            BitSet nonUniqueIndexedColumns,
                            BitSet descColumns,
                            int[] mainColToIndexPosMap,
                            boolean isUnique,
                            boolean isUniqueWithDuplicateNulls,
                            KryoPool kryoPool,
                            int[] columnOrdering,
                            int[] format_ids){
        this.indexedColumns = indexedColumns;
        this.translatedIndexedColumns = translatedIndexedColumns;
        this.nonUniqueIndexedColumns = nonUniqueIndexedColumns;
        this.descColumns = descColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
        this.kryoPool = kryoPool;
        this.columnOrdering = columnOrdering;
        this.format_ids = format_ids;
        pkColumns = new BitSet();
        nonPkColumns = new BitSet();

        if(columnOrdering != null && columnOrdering.length > 0) {
            for (int col:columnOrdering) {
                pkColumns.set(col);
            }
        }


        nonPkColumns = (BitSet)pkColumns.clone();
        nonPkColumns.flip(0, nonPkColumns.size());
    }

    public static IndexTransformer newTransformer(BitSet indexedColumns,
                                                  int[] mainColToIndexPosMap,
                                                  BitSet descColumns,
                                                  boolean unique,
                                                  boolean uniqueWithDuplicateNulls,
                                                  KryoPool kryoPool,
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
                                    kryoPool,
                                    columnOrdering,
                                    format_ids
        );
    }

    public static IndexTransformer newTransformer(BitSet indexedColumns,
                                                  int[] mainColToIndexPosMap,
                                                  BitSet descColumns,
                                                  boolean unique,
                                                  boolean uniqueWithDuplicateNulls,
                                                  int[] columnOrdering,
                                                  int[] format_ids){
        return newTransformer(indexedColumns, mainColToIndexPosMap,
                descColumns, unique, uniqueWithDuplicateNulls,SpliceDriver.getKryoPool(),columnOrdering,format_ids);
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
            reverseColumnOrdering = new int[format_ids.length];
            for (int i = 0; i < columnOrdering.length; ++i){
                reverseColumnOrdering[columnOrdering[i]] = i;
            }
        }

        if (idvds == null) {
            int n = (int)indexedColumns.cardinality();
            idvds = new DataValueDescriptor[n];
            int pos = 0;
            for (int i = 0; i < format_ids.length; ++i){
                if (indexedColumns.get(i)) {
                    idvds[pos++] = LazyDataValueFactory.getLazyNull(format_ids[i]);
                }
            }
        }
    }

    private void createKeyDecoder() {
        if (keyDecoder == null)
            keyDecoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());
        else
            keyDecoder.reset();
    }

    public KVPair translate(KVPair mutation) throws IOException, StandardException {
        if (mutation == null) return null; //nothing to do

        buildKeyMap(mutation);

        //make sure that row and key accumulators are initialized
        getRowAccumulator();
        getKeyAccumulator();

        if (mainPutDecoder == null)
            mainPutDecoder = new EntryDecoder(kryoPool);

        mainPutDecoder.set(mutation.getValue());

        BitIndex mutationIndex = mainPutDecoder.getCurrentIndex();
        MultiFieldDecoder mutationDecoder = mainPutDecoder.getEntryDecoder();

        // Check for null columns in data when isUniqueWithDuplicateNulls == true
        // -- in this case, we'll have to append a uniqueness value to the row key
        boolean makeUniqueForDuplicateNulls = false;

        // fill in index columns from mutation row
        for (int i = mutationIndex.nextSetBit(0); i >= 0 && i < indexedColumns.length(); i = mutationIndex.nextSetBit(i + 1)) {
            if (indexedColumns.get(i)) {
                if (isUniqueWithDuplicateNulls && !makeUniqueForDuplicateNulls) {
                    makeUniqueForDuplicateNulls = mainPutDecoder.nextIsNull(i);
                }
                int mappedPosition = mainColToIndexPosMap[i];
                ByteSlice slice = ByteSlice.empty();
                mainPutDecoder.nextField(mutationDecoder,i,slice);
                ByteSlice keySlice = indexKeyAccumulator.getField(mappedPosition, true);
                keySlice.set(slice, descColumns.get(mappedPosition));
                occupy(indexKeyAccumulator, idvds[mappedPosition], i);

            } else if (nonPkColumns.get(i)){
                mainPutDecoder.seekForward(mutationDecoder, i);
            }
        }

        // fill in index columns from mutation row key
        if (columnOrdering != null && columnOrdering.length > 0) {
            for (int i = 0; i < columnOrdering.length; ++i) {
                int pos = columnOrdering[i];
                if (pos < mainColToIndexPosMap.length) {
                    int mappedPosition = mainColToIndexPosMap[pos];
                    if (indexedColumns.get(pos)) {
                        if (descColumns.get(mappedPosition)) {
                            for (int j = 0; j < pkIndex[i].getSize(); ++j) {
                                keyDecoder.array()[pkIndex[i].getOffset() + j] ^=0xff;
                            }
                        }
                        indexKeyAccumulator.add(mappedPosition,keyDecoder.array(),pkIndex[i].getOffset(),pkIndex[i].getSize());
                        occupy(indexKeyAccumulator,idvds[mappedPosition],pos);
                    }
                }
            }
        }

        // For index key, mark all column as occupied
        BitSet remainingFields = indexKeyAccumulator.getRemainingFields();
        for(int n = remainingFields.nextSetBit(0);n<indexedColumns.length() && n>=0;n=remainingFields.nextSetBit(n+1)) {
            if (isUniqueWithDuplicateNulls && !makeUniqueForDuplicateNulls) {
                makeUniqueForDuplicateNulls = true;
            }
            int mappedPosition = mainColToIndexPosMap[n];
            occupy(indexKeyAccumulator, idvds[mappedPosition], n);
        }

        //add the row location to the end of the index row
        byte[] encodedRow = Encoding.encodeBytesUnsorted(mutation.getRow());
        // only make the call to check the accumulator if we have to -- if we haven't already determined
        makeUniqueForDuplicateNulls = (isUniqueWithDuplicateNulls &&
                (makeUniqueForDuplicateNulls || !indexKeyAccumulator.isFinished()));
        if(isUnique)
            indexRowAccumulator.add((int) translatedIndexedColumns.length(), encodedRow,0,encodedRow.length);
        byte[] indexRowKey = getIndexRowKey(encodedRow, (!isUnique || makeUniqueForDuplicateNulls));
        byte[] indexRowData = indexRowAccumulator.finish();
        return new KVPair(indexRowKey, indexRowData, mutation.getType());
    }

    private void occupy(EntryAccumulator accumulator, DataValueDescriptor dvd, int position) {
        int mappedPosition = mainColToIndexPosMap[position];
        if(DerbyBytesUtil.isScalarType(dvd, null))
            accumulator.markOccupiedScalar(mappedPosition);
        else if(DerbyBytesUtil.isDoubleType(dvd))
            accumulator.markOccupiedDouble(mappedPosition);
        else if(DerbyBytesUtil.isFloatType(dvd))
            accumulator.markOccupiedFloat(mappedPosition);
        else
            accumulator.markOccupiedUntyped(mappedPosition);
    }

    public byte[] getIndexRowKey(byte[] rowKey, boolean makeUniqueRowKey){
        if(makeUniqueRowKey)
            indexKeyAccumulator.add((int) translatedIndexedColumns.length(), rowKey, 0, rowKey.length);

        return indexKeyAccumulator.finish();
    }


    public ByteEntryAccumulator getRowAccumulator() {
        if (indexRowAccumulator == null)
            indexRowAccumulator = new ByteEntryAccumulator(null, true, nonUniqueIndexedColumns);
        else
            indexRowAccumulator.reset();
        return indexRowAccumulator;
    }

    public ByteEntryAccumulator getKeyAccumulator() {
        if (indexKeyAccumulator == null)
            indexKeyAccumulator = new ByteEntryAccumulator(null, false, isUnique ? translatedIndexedColumns : nonUniqueIndexedColumns);
        else
            indexKeyAccumulator.reset();
        return indexKeyAccumulator;
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