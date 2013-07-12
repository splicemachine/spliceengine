package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.*;
import org.xerial.snappy.Snappy;

import java.io.IOException;
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

    public void set(byte[] bytes){
        this.data = bytes;
        rebuildBitIndex();
    }

    private void rebuildBitIndex() {
        //build a new bitIndex from the data
        byte headerByte = data[0];
        if((headerByte & 0x10)!=0){
            bitIndex = AllFullBitIndex.INSTANCE;
            compressedData = (headerByte & 0x20) !=0;
            dataOffset=2;
            return;
        }
        //find separator byte
        dataOffset = 0;
        for(int i=0;i<data.length;i++){
            if(data[i]==0x00){
                dataOffset = i;
                break;
            }
        }
        if((headerByte & 0x80) !=0){
           if((headerByte & 0x40)!=0){
               bitIndex = BitIndexing.compressedBitMap(data, 0, dataOffset);
           }else{
               bitIndex = BitIndexing.uncompressedBitMap(data, 0, dataOffset);
           }
        }else{
            //sparse index
            bitIndex = BitIndexing.sparseBitMap(data, 0, dataOffset);
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
        for(start = dataOffset;start<data.length&&fieldSkipped<fieldsToSkip;start++){
            if(data[start]==0x00){
                fieldSkipped++;
            }
        }

        //seek until we hit the next terminator
        int stop;
        for(stop = start;stop<data.length;stop++){
            if(data[stop]==0x00){
                break;
            }
        }

        if(stop>data.length)
            stop = data.length;
        int length = stop-start;
        byte[] retData = new byte[length];
        System.arraycopy(data,start,retData,0,length);
        return retData;
    }

    private void decompressIfNeeded() throws IOException {
//        if(compressedData){
//            //uncompress the data, then flag it off so we don't keep uncompressing
//            byte[] uncompressed = new byte[Snappy.uncompressedLength(data, dataOffset, data.length - dataOffset)];
//            Snappy.uncompress(data,dataOffset,data.length-dataOffset,uncompressed,0);
//            data = uncompressed;
//            compressedData=false;
//            dataOffset=0;
//        }
    }

    public MultiFieldDecoder getEntryDecoder() throws IOException{
        decompressIfNeeded();
        MultiFieldDecoder wrap = MultiFieldDecoder.wrap(data);
        wrap.seek(dataOffset); //position self correctly in array

        return wrap;
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
            decoder.seek(decoder.offset() + 5);
        }else if(bitIndex.isDoubleType(position)){
            //doubles are always 8 bytes, so skip the after delimiter as well
            decoder.seek(decoder.offset()+ 9);
        }else
            decoder.skip();
    }
}
