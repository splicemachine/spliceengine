package com.splicemachine.storage;


import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.index.*;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/5/13
 */
public class EntryEncoder {
    private final BitIndex bitIndex;
    /*
     * The threshold at which we compress the data. If the data length is less than
     * this, we don't compress, but if it's greater, we do.
     *
     * This value is chosen so that Snappy Compression will actually accomplish something.
     * Adjust it up or down based on empirical results for the individual compressor
     */
    private static final int DATA_COMPRESSION_THRESHOLD=150;
    /*
     * The bit to indicate whether or not the data has been compressed.
     */
    private static final byte COMPRESSED_DATA_BIT = 0x20;

    private MultiFieldEncoder encoder;


    private EntryEncoder(BitIndex bitIndex){
        this.bitIndex = bitIndex;
        this.encoder = MultiFieldEncoder.create(bitIndex.cardinality());
    }

    public EntryEncoder(int size, AllFullBitIndex allFullBitIndex) {
        this.encoder = MultiFieldEncoder.create(size);
        this.bitIndex = allFullBitIndex;
    }

    public MultiFieldEncoder getEntryEncoder(){
        if(encoder==null)
            encoder = MultiFieldEncoder.create(bitIndex.cardinality());
        return encoder;
    }

    public byte[] encode() throws IOException {
        byte[] finalData = encoder.build();

        byte[] bitData = bitIndex.encode();
        if(finalData.length>DATA_COMPRESSION_THRESHOLD){
            finalData = Snappy.compress(finalData);
            //mark the header bit for compressed data
            bitData[0] = (byte)(bitData[0] | COMPRESSED_DATA_BIT);
        }

        byte[] entry = new byte[bitData.length+finalData.length+1];
        System.arraycopy(bitData, 0, entry, 0, bitData.length);
        entry[bitData.length] = 0;
        System.arraycopy(finalData,0,entry,bitData.length+1,finalData.length);

        return entry;
    }

    public static EntryEncoder create(int numCols,int[] setCols){
        BitSet cols = new BitSet(numCols);
        for(int col:setCols){
            cols.set(col);
        }

        return create(numCols, cols);
    }

    public static EntryEncoder create(int numCols, BitSet setCols){
        if(setCols==null||setCols.cardinality()==numCols){
            /*
             * This is a special case where we are writing *every* column. In this case, we just
             * set an indicator flag that tells us to not bother reading the index, because everything
             * is there.
             */
            return new EntryEncoder(numCols,new AllFullBitIndex());
        }else{

            BitIndex indexToUse = BitIndexing.uncompressedBitMap(setCols);
            //see if we can improve space via compression
            BitIndex denseCompressedBitIndex = DenseCompressedBitIndex.compress(setCols);
            if(denseCompressedBitIndex.length() < indexToUse.length()){
                indexToUse = denseCompressedBitIndex;
            }
            //see if sparse is better
            BitIndex sparseBitMap = BitIndexing.sparseBitMap(setCols);
            if(sparseBitMap.length()<indexToUse.length()){
                indexToUse = sparseBitMap;
            }
            return new EntryEncoder(indexToUse);
        }
    }

    public static void main(String... args)throws Exception{
        BitSet bitSet = new BitSet(10);
        bitSet.set(0);
        bitSet.set(3);
        bitSet.set(7);
        bitSet.set(9);
        EntryEncoder entryEncoder = EntryEncoder.create(10, bitSet);
        byte[] bytes = entryEncoder.encode();
        System.out.println(Arrays.toString(bytes));
    }
}

