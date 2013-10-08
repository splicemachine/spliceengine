package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.*;
import com.splicemachine.utils.kryo.KryoPool;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class EntryDecoder {
    private BitIndex bitIndex;

    private byte[] data;
    private boolean compressedData = false;
    private int dataOffset;
    private MultiFieldDecoder decoder;
    private final KryoPool kryoPool;
    private int offset;
    private int length;

    public EntryDecoder(KryoPool kryoPool) {
        this.kryoPool = kryoPool;
    }

    public void set(byte[] bytes){
        set(bytes,0,bytes.length);
    }

    public void set(byte[] bytes, int offset,int length){
        this.data = bytes;
        this.length = length;
        this.offset = offset;

        rebuildBitIndex();
        if(decoder!=null)
            decoder.set(bytes,offset+dataOffset, length-dataOffset);
    }

    private void rebuildBitIndex() {
        //find separator byte
        dataOffset = 0;
        for(int i=offset;i<offset+length;i++){
            if(data[i]==0x00){
                dataOffset = i-offset;
                break;
            }
        }
        //build a new bitIndex from the data
        byte headerByte = data[offset];
        if((headerByte & 0x80) !=0){
           if((headerByte & 0x40)!=0){
               bitIndex = BitIndexing.compressedBitMap(data, offset, dataOffset);
           }else{
               bitIndex = BitIndexing.uncompressedBitMap(data, offset, dataOffset);
           }
        }else{
            //sparse index
            bitIndex = BitIndexing.sparseBitMap(data, offset, dataOffset);
        }

        dataOffset++;
        compressedData = (headerByte & 0x20) != 0;
    }

    public boolean isSet(int position){
        return bitIndex.isSet(position);
    }

    public BitIndex getCurrentIndex(){
        return bitIndex;
    }

    public byte[] getData(int position) throws IOException {
        if(!isSet(position)) throw new NoSuchElementException();

        decompressIfNeeded();
        //get number of fields to skip
        int fieldsToSkip =bitIndex.cardinality(position);
        int fieldSkipped=0;
        int start;
        for(start = dataOffset;start<length&&fieldSkipped<fieldsToSkip;start++){
            if(data[start]==0x00){
                fieldSkipped++;
            }
        }

        //seek until we hit the next terminator
        int stop;
        for(stop = start;stop<length;stop++){
            if(data[stop]==0x00){
                break;
            }
        }

        if(stop>length)
            stop = length;
        int length = stop-start;
        byte[] retData = new byte[length];
        System.arraycopy(data,start,retData,0,length);
        return retData;
    }

    private void decompressIfNeeded() throws IOException {
//        if(compressedData){
//            //uncompress the data, then flag it off so we don't keep uncompressing
//            byte[] uncompressed = new byte[Snappy.uncompressedLength(data, dataOffset, length - dataOffset)];
//            Snappy.uncompress(data,dataOffset,length-dataOffset,uncompressed,0);
//            data = uncompressed;
//            compressedData=false;
//            dataOffset=0;
//        }
    }

    public MultiFieldDecoder getEntryDecoder() throws IOException{
        decompressIfNeeded();
        if(decoder==null){
            decoder = MultiFieldDecoder.wrap(data,offset+dataOffset,length-dataOffset,kryoPool);
        }
        decoder.seek(offset+dataOffset);
        return decoder;
    }

    public void seekForward(MultiFieldDecoder decoder,int position) {
    /*
     * Certain fields may contain zeros--in particular, scalar, float, and double types. We need
     * to skip past those zeros without treating them as delimiters. Since we have that information
     * in the index, we can simply decode and throw away the proper type to adjust the offset properly.
     * However, in some cases it's more efficient to skip the count directly, since we may know the
     * byte size already.
     */
        if(bitIndex.isScalarType(position)){
            decoder.decodeNextLong(); //don't need the value, just need to seek past it
        }else if(bitIndex.isFloatType(position)){
            //floats are always 4 bytes, so skip the after delimiter
            decoder.skipFloat();
        }else if(bitIndex.isDoubleType(position)){
            decoder.skipDouble();
        }else
            decoder.skip();
    }

    public ByteBuffer nextAsBuffer(MultiFieldDecoder decoder,int position) {
        int offset = decoder.offset();
        seekForward(decoder,position);
        int length = decoder.offset()-1-offset;
        if(length<=0) return null;

        return ByteBuffer.wrap(decoder.array(),offset,length);
    }

    public void accumulate(int position, ByteBuffer buffer,EntryAccumulator accumulator) {
        if(bitIndex.isScalarType(position)){
            accumulator.addScalar(position,buffer);
        }else if(bitIndex.isFloatType(position)){
            accumulator.addFloat(position,buffer);
        }else if(bitIndex.isDoubleType(position))
            accumulator.addDouble(position,buffer);
        else
            accumulator.add(position,buffer);
    }

    public void close(){
        if(decoder!=null)
            decoder.close();
    }

}
