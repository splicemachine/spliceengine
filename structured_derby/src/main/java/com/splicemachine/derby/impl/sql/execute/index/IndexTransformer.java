package com.splicemachine.derby.impl.sql.execute.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.SparseEntryAccumulator;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.kryo.KryoPool;

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
    private EntryAccumulator indexKeyAccumulator;
    private EntryAccumulator indexRowAccumulator;
    private EntryDecoder mainPutDecoder;

    public IndexTransformer(BitSet indexedColumns,
                            BitSet translatedIndexedColumns,
                            BitSet nonUniqueIndexedColumns,
                            BitSet descColumns,
                            int[] mainColToIndexPosMap,
                            boolean isUnique,
                            boolean isUniqueWithDuplicateNulls,KryoPool kryoPool) {
        this.indexedColumns = indexedColumns;
        this.translatedIndexedColumns = translatedIndexedColumns;
        this.nonUniqueIndexedColumns = nonUniqueIndexedColumns;
        this.descColumns = descColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.isUnique = isUnique;
        this.isUniqueWithDuplicateNulls = isUniqueWithDuplicateNulls;
				this.kryoPool = kryoPool;
    }


		public static IndexTransformer newTransformer(BitSet indexedColumns,
																									int[] mainColToIndexPosMap,
																									BitSet descColumns,
																									boolean unique,
																									boolean uniqueWithDuplicateNulls,
																									KryoPool kryoPool) {
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
								kryoPool
				);
		}
    public static IndexTransformer newTransformer(BitSet indexedColumns,
                                                  int[] mainColToIndexPosMap,
                                                  BitSet descColumns,
                                                  boolean unique,
                                                  boolean uniqueWithDuplicateNulls) {
				return newTransformer(indexedColumns, mainColToIndexPosMap,
								descColumns, unique, uniqueWithDuplicateNulls,SpliceDriver.getKryoPool());
    }

    public KVPair translate(KVPair mutation) throws IOException {
        if (mutation == null) return null; //nothing to do

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

        for (int i = mutationIndex.nextSetBit(0); i >= 0 && i <= indexedColumns.length(); i = mutationIndex.nextSetBit(i + 1)) {
            if (indexedColumns.get(i)) {
                if (isUniqueWithDuplicateNulls && !makeUniqueForDuplicateNulls) {
                    makeUniqueForDuplicateNulls = mainPutDecoder.nextIsNull(i);
                }
								int mappedPosition = mainColToIndexPosMap[i];
								ByteSlice rowSlice = indexRowAccumulator.getField(mappedPosition, true);
								mainPutDecoder.nextField(mutationDecoder,i,rowSlice);

								ByteSlice keySlice = indexKeyAccumulator.getField(mappedPosition, true);
								keySlice.set(rowSlice,descColumns.get(mappedPosition));
								occupy(indexKeyAccumulator,mutationIndex,i);
								occupy(indexRowAccumulator,mutationIndex,i);
            } else {
                mainPutDecoder.seekForward(mutationDecoder, i);
            }
        }
        //add the row location to the end of the index row
				byte[] encodedRow = Encoding.encodeBytesUnsorted(mutation.getRow());
				indexRowAccumulator.add((int) translatedIndexedColumns.length(), encodedRow,0,encodedRow.length);
        // only make the call to check the accumulator if we have to -- if we haven't already determined
        makeUniqueForDuplicateNulls = (isUniqueWithDuplicateNulls &&
                (makeUniqueForDuplicateNulls || !indexKeyAccumulator.isFinished()));
        byte[] indexRowKey = getIndexRowKey(mutation.getRow(), (!isUnique || makeUniqueForDuplicateNulls));
        byte[] indexRowData = indexRowAccumulator.finish();
        return new KVPair(indexRowKey, indexRowData, mutation.getType());
    }

		private void occupy(EntryAccumulator accumulator, BitIndex index, int position) {
			int mappedPosition = mainColToIndexPosMap[position];
				if(index.isScalarType(position))
						accumulator.markOccupiedScalar(mappedPosition);
				else if(index.isDoubleType(position))
						accumulator.markOccupiedDouble(mappedPosition);
				else if(index.isFloatType(position))
						accumulator.markOccupiedFloat(mappedPosition);
				else
						accumulator.markOccupiedUntyped(mappedPosition);
		}


		public byte[] getIndexRowKey(byte[] rowKey, boolean makeUniqueRowKey){
        if(makeUniqueRowKey)
            indexKeyAccumulator.add((int) translatedIndexedColumns.length(), rowKey, 0, rowKey.length);

        return indexKeyAccumulator.finish();
    }

		public EntryAccumulator getRowAccumulator() {
        if (indexRowAccumulator == null)
            indexRowAccumulator = new SparseEntryAccumulator(null, nonUniqueIndexedColumns, true);
				else
					indexRowAccumulator.reset();
        return indexRowAccumulator;
    }

    public EntryAccumulator getKeyAccumulator() {
        if (indexKeyAccumulator == null)
            indexKeyAccumulator = new SparseEntryAccumulator(null, isUnique ? translatedIndexedColumns : nonUniqueIndexedColumns, false);
				else
					indexKeyAccumulator.reset();
        return indexKeyAccumulator;
    }

    public boolean isUnique() {
        return isUnique;
    }
}
