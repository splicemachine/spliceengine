package com.splicemachine.derby.impl.sql.execute.index;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.SparseEntryAccumulator;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.Snowflake;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * Class responsible for transforming an incoming main table row
 * into an index row
 *
 * @author Scott Fines
 * Created on: 8/23/13
 */
public class IndexTransformer {
    private EntryAccumulator indexKeyAccumulator;
    private EntryAccumulator indexRowAccumulator;
    private EntryDecoder mainPutDecoder;

    private final BitSet indexedColumns;
    private final BitSet nonUniqueIndexedColumns;
    private final BitSet translatedIndexedColumns;

    private final boolean isUnique;
    private final int[] mainColToIndexPosMap;
    private final BitSet descColumns;
    private Snowflake.Generator generator;
    private final int blockSize;

    public static IndexTransformer newTransformer(BitSet indexedColumns,
                            int[] mainColToIndexPosMap,
                            BitSet descColumns,
                            boolean unique,
                            int blockSize) {


        BitSet translatedIndexedColumns = new BitSet(indexedColumns.cardinality());
        for(int i = indexedColumns.nextSetBit(0);i>=0;i=indexedColumns.nextSetBit(i+1)){
            translatedIndexedColumns.set(mainColToIndexPosMap[i]);
        }
        BitSet nonUniqueIndexedColumns = (BitSet)translatedIndexedColumns.clone();
        nonUniqueIndexedColumns.set(translatedIndexedColumns.length());

        return new IndexTransformer(indexedColumns,
                translatedIndexedColumns,
                nonUniqueIndexedColumns,
                descColumns,
                mainColToIndexPosMap,
                unique,
                blockSize);
    }

    public IndexTransformer(BitSet indexedColumns,
                            BitSet translatedIndexedColumns,
                            BitSet nonUniqueIndexedColumns,
                            BitSet descColumns,
                            int[] mainColToIndexPosMap,
                            boolean isUnique,
                            int blockSize){
        this.indexedColumns = indexedColumns;
        this.translatedIndexedColumns = translatedIndexedColumns;
        this.nonUniqueIndexedColumns = nonUniqueIndexedColumns;
        this.descColumns = descColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.isUnique = isUnique;
        this.blockSize = blockSize;
    }

    public KVPair translate(KVPair mutation) throws IOException {
        if(mutation==null) return null; //nothing to do

        //make sure that row and key accumulators are initialized
        getRowAccumulator();
        getKeyAccumulator();

        indexKeyAccumulator.reset();
        indexRowAccumulator.reset();

        if(mainPutDecoder==null)
            mainPutDecoder = new EntryDecoder(SpliceDriver.getKryoPool());

        mainPutDecoder.set(mutation.getValue());

        BitIndex mutationIndex = mainPutDecoder.getCurrentIndex();
        MultiFieldDecoder mutationDecoder = mainPutDecoder.getEntryDecoder();
        for(int i=mutationIndex.nextSetBit(0);i>=0&&i<=indexedColumns.length();i=mutationIndex.nextSetBit(i+1)){
            if(indexedColumns.get(i)){
                ByteBuffer entry = mainPutDecoder.nextAsBuffer(mutationDecoder, i);
                if(descColumns.get(mainColToIndexPosMap[i]))
                    accumulate(indexKeyAccumulator,mutationIndex,getDescendingBuffer(entry),i);
                else
                    accumulate(indexKeyAccumulator,mutationIndex,entry,i);
                accumulate(indexRowAccumulator,mutationIndex,entry,i);
            }else{
                mainPutDecoder.seekForward(mutationDecoder,i);
            }
        }
        //add the row location to the end of the index row
        indexRowAccumulator.add(translatedIndexedColumns.length(),ByteBuffer.wrap(Encoding.encodeBytesUnsorted(mutation.getRow())));
        byte[] indexRowKey = getIndexRowKey();
        byte[] indexRowData = indexRowAccumulator.finish();
        return new KVPair(indexRowKey,indexRowData);
    }

    private void accumulate(EntryAccumulator accumulator,BitIndex index,ByteBuffer buffer,int position){
        if(index.isScalarType(position))
            accumulator.addScalar(mainColToIndexPosMap[position],buffer);
        else if(index.isFloatType(position))
            accumulator.addFloat(mainColToIndexPosMap[position],buffer);
        else if(index.isDoubleType(position))
            accumulator.addDouble(mainColToIndexPosMap[position],buffer);
        else
            accumulator.add(mainColToIndexPosMap[position],buffer);
    }

    public byte[] getIndexRowKey(){
        if(isUnique) return indexKeyAccumulator.finish();
        else{
            if(generator==null)
                generator = SpliceDriver.driver().getUUIDGenerator().newGenerator(blockSize);
            byte[] uuid = generator.nextBytes();
            indexKeyAccumulator.add(translatedIndexedColumns.length(),ByteBuffer.wrap(uuid));
            return indexKeyAccumulator.finish();
        }
    }

    protected ByteBuffer getDescendingBuffer(ByteBuffer entry) {
        entry.mark();
        byte[] data = new byte[entry.remaining()];
        entry.get(data);
        entry.reset();
        for(int i=0;i<data.length;i++){
            data[i]^=0xff;
        }
        return ByteBuffer.wrap(data);
    }

    public EntryAccumulator getRowAccumulator() {
        if(indexRowAccumulator==null)
            indexRowAccumulator = new SparseEntryAccumulator(null,nonUniqueIndexedColumns,true);
        return indexRowAccumulator;
    }

    public EntryAccumulator getKeyAccumulator() {
        if(indexKeyAccumulator==null)
            indexKeyAccumulator = new SparseEntryAccumulator(null,isUnique?translatedIndexedColumns: nonUniqueIndexedColumns,false);
        return indexKeyAccumulator;
    }

}
