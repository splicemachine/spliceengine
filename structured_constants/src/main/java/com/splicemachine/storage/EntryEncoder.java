package com.splicemachine.storage;


import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.storage.index.*;
import com.splicemachine.utils.kryo.KryoPool;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;


/**
 * @author Scott Fines
 *         Created on: 7/5/13
 */
public class EntryEncoder {
    private BitIndex bitIndex;
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
    private final KryoPool kryoPool;

    private EntryEncoder(KryoPool kryoPool,BitIndex bitIndex){
        this.bitIndex = bitIndex;
        this.encoder = MultiFieldEncoder.create(kryoPool,bitIndex.cardinality());
        this.kryoPool = kryoPool;
    }

    public MultiFieldEncoder getEntryEncoder(){
        if(encoder==null)
            encoder = MultiFieldEncoder.create(kryoPool,bitIndex.cardinality());
        return encoder;
    }

    public byte[] encode() throws IOException {
        byte[] finalData = encoder.build();

        byte[] bitData = bitIndex.encode();
//        if(finalData.length>DATA_COMPRESSION_THRESHOLD){
//            finalData = Snappy.compress(finalData);
//            //mark the header bit for compressed data
//            bitData[0] = (byte)(bitData[0] | COMPRESSED_DATA_BIT);
//        }

        byte[] entry = new byte[bitData.length+finalData.length+1];
        System.arraycopy(bitData, 0, entry, 0, bitData.length);
        entry[bitData.length] = 0;
        System.arraycopy(finalData,0,entry,bitData.length+1,finalData.length);

        return entry;
    }

    public void reset(BitSet nonNullFields) {
        int oldCardinality = bitIndex.cardinality();
        boolean differs = nonNullFields.cardinality() != oldCardinality;

        if (!differs) {
            for (int i = bitIndex.nextSetBit(0); i >= 0; i = bitIndex.nextSetBit(i + 1)) {
                if (!nonNullFields.get(i)) {
                    differs = true;
                    break;
                }
            }
        }

        if (differs) {
            bitIndex = BitIndexing.getBestIndex(nonNullFields, bitIndex.getScalarFields(),
                                                   bitIndex.getFloatFlields(), bitIndex.getDoubleFields());
        }
        resetEncoder();
    }

    public void reset(BitIndex newIndex) {
        this.bitIndex = newIndex;
        resetEncoder();
    }

    private void resetEncoder() {
        if (encoder == null) return;
                /*
				 * It is possible that we changed the underlying bitset in the bitIndex out from under
				 * us, which means that the encoder is using out of date information. Check for that,
				 * and if so, reset the encoder
				 */

        if (encoder.getNumFields() == bitIndex.cardinality()) {
            encoder.reset();
        } else {
            //close the old encoder to prevent resource leaks
            encoder.close();
            encoder = MultiFieldEncoder.create(kryoPool, bitIndex.cardinality());
        }
    }

    public void reset(BitSet newIndex,BitSet newScalarFields,BitSet newFloatFields,BitSet newDoubleFields){
        //save effort if they are the same
        int oldCardinality = bitIndex.cardinality();
        boolean differs=newIndex.cardinality()!=oldCardinality;

        if(!differs){
            for(int i=newIndex.nextSetBit(0);i>=0;i=newIndex.nextSetBit(i+1)){
                if(!bitIndex.isSet(i)){
                    differs=true;
                    break;
                }else if(newScalarFields.get(i)!=bitIndex.isScalarType(i)){
                    differs=true;
                    break;
                }else if(newFloatFields.get(i)!=bitIndex.isFloatType(i)){
                    differs=true;
                    break;
                }else if(newDoubleFields.get(i)!=bitIndex.isDoubleType(i)){
                    differs=true;
                    break;
                }
            }

            for(int i=bitIndex.nextSetBit(0);i>=0;i=bitIndex.nextSetBit(i+1)){
                if(!newIndex.get(i)){
                    differs=true;
                    break;
                }
            }
        }
        if(differs){
            bitIndex = BitIndexing.getBestIndex(newIndex, newScalarFields, newFloatFields, newDoubleFields);
        }

				resetEncoder();
    }

    public void close(){
        if(encoder!=null)
            encoder.close();
    }

    public static EntryEncoder create(KryoPool kryoPool,int numCols, BitSet setCols,BitSet scalarFields,BitSet floatFields,BitSet doubleFields){
        //TODO -sf- return all full stuff as well
        BitIndex indexToUse = BitIndexing.getBestIndex(setCols, scalarFields, floatFields, doubleFields);
        return new EntryEncoder(kryoPool,indexToUse);
    }

    public static EntryEncoder create(KryoPool kryoPool,BitIndex newIndex){
        return new EntryEncoder(kryoPool,newIndex);
    }


}

