package com.splicemachine.storage;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
public class BitWriter {
    private byte[] buffer;

    private int[] byteAndBitOffset;

    private int length;
    private boolean useContinuationBit;

    public BitWriter(byte[] buffer, int offset, int length, int initialBitPos,
                     boolean useContinuationBit){
        this.buffer = buffer;
        this.length = length;
        this.byteAndBitOffset = new int[]{offset,initialBitPos};
        this.useContinuationBit = useContinuationBit;
    }

    public void set(byte[] buffer, int offset, int length, int initialBitPos){
        this.buffer = buffer;
        this.byteAndBitOffset[0] = offset;
        byteAndBitOffset[1] = initialBitPos;
        this.length = length;
    }

    public void setNext(){
        set(1);
    }

    /**
     * Set the next {@code n} bits to 1.
     *
     * @param n the number of bits to set to 1
     */
    public void set(int n){
        for(int i=0;i<n;i++){
            if(byteAndBitOffset[1]==9){
                byteAndBitOffset[0]++;
                if(byteAndBitOffset[0]>length) throw new IndexOutOfBoundsException();
                if(useContinuationBit){
                    buffer[byteAndBitOffset[0]] = (byte)0x80;
                    byteAndBitOffset[1] = 2;
                }else{
                    byteAndBitOffset[1] = 1;
                }
            }
            //TODO -sf- we could group this up into byte and word-boundaries and be
            //more efficient
            buffer[byteAndBitOffset[0]] |= (1<<Byte.SIZE-byteAndBitOffset[1]);
            byteAndBitOffset[1]++;
        }
    }

    public void skipNext(){
        skip(1);
    }

    public void skip(int n){
        for(int i=0;i<n;i++){
            if(byteAndBitOffset[1]==9){
                byteAndBitOffset[0]++;
                if(byteAndBitOffset[0]>length) throw new IndexOutOfBoundsException();
                if(useContinuationBit){
                    buffer[byteAndBitOffset[0]] = (byte)0x80;
                    byteAndBitOffset[1] = 2;
                }else{
                    byteAndBitOffset[1] = 1;
                }
            }
            byteAndBitOffset[1]++;
        }
    }
}
