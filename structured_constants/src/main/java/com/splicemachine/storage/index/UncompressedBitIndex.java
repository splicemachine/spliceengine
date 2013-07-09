package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * Uncompressed, variable-length BitIndex.
 *
 * This index is represented as follows:
 *
 * The first four bits are header bits
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
class UncompressedBitIndex implements BitIndex {
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
    public int cardinality(int position) {
        int count=0;
        for(int i=bitSet.nextSetBit(0);i>=0&&i<position;i=bitSet.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        byte headerByte = (byte)0x80;
        //fill the first byte specially
        int setPos;
        for(setPos = bitSet.nextSetBit(0);setPos>=0&&setPos<4;setPos=bitSet.nextSetBit(setPos+1)){
            if(setPos==0){
                headerByte |= 0x8;
            }else if(setPos==1){
                headerByte |= 0x4;
            }else if(setPos==2){
                headerByte |= 0x2;
            }else if(setPos==3){
                headerByte |= 0x1;
            }
        }

        if(setPos<0){
            //we've populated the entire bitset, just return it
            return new byte[]{headerByte};
        }

        byte[] bytes = new byte[encodedSize()];
        //set the header bits
        bytes[0] = headerByte;
        for(int i=1;i<bytes.length;i++){
            bytes[i] = (byte)0x80; //set a 1 to indicate bitset membership
        }

        int bitPos;
        while(setPos>=0){
            /*
             * Each setPos is > 3 (since 0,1,2,3 are packed into the header byte).
             *
             * We need to find which byte this position belongs to. We adjust the position down by four, to
             * account for the header bits, making setPos-4.Since there are 7 bits per additional byte,
             * the byte index would be setPos/7. Since the header byte is special, we add one to offset, giving
             * (setPos-4)/7+1 for the byteIndex.
             *
             * Within the byte, there are 7 available positions, so we take (setPos-4)%7. Because of the continuation
             * bit, we shift these values up by 1. This gives us the left-to-right position of the bit within
             * the bytes. We then shift to place a 1 in that position (shifts are always 1<<(x-1)), so we end up
             * shifting as 1<<Byte.SIZE-((setPos-4)%7+1)-1
             *
             */
            bitPos = setPos-4;
            int byteIndex = (bitPos/7)+1;
            bitPos = (bitPos%7+1);
            bytes[byteIndex] |= (1<<Byte.SIZE-bitPos-1);
            setPos = bitSet.nextSetBit(setPos+1);
        }
        return bytes;
    }

    @Override
    public int nextSetBit(int position) {
        return bitSet.nextSetBit(position);
    }

    @Override
    public int encodedSize() {
        /*
         * The number of bytes goes as follows:
         *
         * you need at least as many bits as the highest 1-bit in the bitSet(equivalent
         *  to bitSet.length()).
         *
         *  you can take 4 bits away because you can fit the first four bits in the header byte
         *
         *  so the total number of bits needed (in addition to the header) is bitSet.length()-4.
         *
         *  There are 7 bits in each byte (ignore the continuation byte at the first entry). Thus, the total
         *  number of bytes should be bitSet.length()-4/7. If bitSet.length()-4 < 7, then it is 2.
         */
        int length = bitSet.length()-4; //four bits have already been set
        int numBytes = length/7;
        if(length%7!=0){
            numBytes++;
        }
        numBytes++; //add the header byte
        return numBytes;
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UncompressedBitIndex)) return false;

        UncompressedBitIndex that = (UncompressedBitIndex) o;

        return bitSet.equals(that.bitSet);
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode();
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return this.bitSet.intersects(bitSet);
    }

    @Override
    public BitSet and(BitSet bitSet) {
        final BitSet result = (BitSet) this.bitSet.clone();
        result.and(bitSet);
        return result;
    }

    public static BitIndex create(BitSet setCols) {
        return new UncompressedBitIndex(setCols);
    }

    public static BitIndex wrap(byte[] data, int position, int limit) {
        //create a BitSet underneath
        BitSet bitSet = new BitSet();
        //set the header entries
        byte headerByte = data[position];
        for(int i=5;i<=8;i++){
            bitSet.set(i-5,(headerByte&(1<<Byte.SIZE-i))!=0);
        }

        //set any additional bytes
        for(int i=1;i<limit;i++){
            byte posByte = data[position+i];
            int bitPos = (i-1)*7+4;
            for(int pos=2;pos<=8;pos++){
                bitSet.set(bitPos+pos-2,(posByte & (1<<Byte.SIZE-pos))!=0);
            }
        }
        return new UncompressedBitIndex(bitSet);
    }

    public static void main(String... args) throws Exception{
        BitSet bitSet = new BitSet(11);
        bitSet.set(0);
        bitSet.set(1);
        bitSet.set(3);
        bitSet.set(4);

        BitIndex bits = UncompressedBitIndex.create(bitSet);

        byte[] data =bits.encode();

        BitIndex decoded = UncompressedBitIndex.wrap(data,0,data.length);
        System.out.println(decoded);
    }
}
