package com.splicemachine.storage.index;

/**
 * Lazily decoding version of a Dense, Compressed Bit Index.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class LazyCompressedBitIndex extends LazyBitIndex{
    private int lastSetPos=0;

    protected LazyCompressedBitIndex(byte[] encodedBitMap,
                                     int offset, int length) {
        super(encodedBitMap, offset, length,5);
    }

    @Override
    protected int decodeNext() {
        if(!bitReader.hasNext()) return -1;
        int next = bitReader.next();
        if(next==0){
            //reading a sequence of unset values
            int numZeros = DeltaCoding.decode(bitReader);
            if(numZeros<0)
                return numZeros;

            lastSetPos+=numZeros;
            if(!bitReader.hasNext()) return -1;
            bitReader.next(); //next field is a 1, so ignore it
        }

        if(!bitReader.hasNext())
            return -1;
        int pos = lastSetPos;
        //get the type from the next two bits

        if(bitReader.next()!=0){
            //either a float or a scalar
            if(bitReader.next()!=0){
                //scalar type
                int numScalars = DeltaCoding.decode(bitReader);
                decodedBits.set(lastSetPos, lastSetPos + numScalars);
                setScalarRange(lastSetPos, lastSetPos + numScalars);
                lastSetPos+=numScalars;
            }else{
                //float type
                int count = DeltaCoding.decode(bitReader);
                decodedBits.set(lastSetPos,lastSetPos+count);
                setFloatRange(lastSetPos,lastSetPos+count);
                lastSetPos+=count;
            }
        }else{
            if(bitReader.next()!=0){
                int numDoubles = DeltaCoding.decode(bitReader);
                decodedBits.set(lastSetPos,lastSetPos+numDoubles);
                setDoubleRange(lastSetPos,lastSetPos+numDoubles);
                lastSetPos+=numDoubles;
            }else{
                int numUntyped = DeltaCoding.decode(bitReader);
                decodedBits.set(lastSetPos,lastSetPos+numUntyped);
                lastSetPos+=numUntyped;
            }
        }
        return pos;
    }

	@Override
	public boolean isCompressed() {
		return true;
	}


}
