package com.splicemachine.storage;

import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/5/13
 */
public class UncompressedBitIndex implements BitIndex {
    private final BitSet bitSet;

    private UncompressedBitIndex(BitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Override
    public int length() {
        return bitSet.length();
    }

    @Override
    public int cardinality() {
        return bitSet.cardinality();
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        int length = bitSet.length()+4;
        int numBytes = bitSet.length()/8;
        if(length%8!=0){
            numBytes++;
        }
        byte[] bytes = new byte[numBytes];
        //set the header bits
        bytes[0] = (byte)0x80;

        //fill the first byte specially
        int setPos;
        for(setPos = bitSet.nextSetBit(0);setPos>=0&&setPos<4;setPos=bitSet.nextSetBit(setPos+1)){
            if(setPos==0){
                bytes[0] |= 0x8;
            }else if(setPos==1){
                bytes[0] |= 0x4;
            }else if(setPos==2){
                bytes[0] |= 0x2;
            }else if(setPos==3){
                bytes[0] |= 0x1;
            }
        }
        int bitPos;
        while(setPos>=0){
            bitPos = setPos+5;
            int byteIndex = bitPos/8;
            int bitPosition = bitPos%8;
            bytes[byteIndex] |= (1<<Byte.SIZE-bitPosition);
            setPos = bitSet.nextSetBit(setPos+1);
        }
        return bytes;
    }

    public static BitIndex create(BitSet setCols) {
        return new UncompressedBitIndex(setCols);
    }

    public static void main(String... args) throws Exception{
        BitSet bitSet = new BitSet(10);
        bitSet.set(2);
        bitSet.set(5);
        bitSet.set(8);
        bitSet.set(9);

        byte[] data = new byte[2];
        data[0] = (byte)0x80;
        int setPos = bitSet.nextSetBit(0)+5;
        int byteIndex = setPos/8;
        data[byteIndex] |= (1<<Byte.SIZE-setPos);
        System.out.println(data[0]);
    }
}
