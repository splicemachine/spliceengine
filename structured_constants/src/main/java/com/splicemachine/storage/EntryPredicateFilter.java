package com.splicemachine.storage;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class EntryPredicateFilter implements Externalizable{
    private static final long serialVersionUID = 1l;
    private BitSet fieldsToReturn;
    private List<Predicate> valuePredicates;

    /**
     * Used for Serialization, DO NOT USE
     */
    @Deprecated
    public EntryPredicateFilter(){}

    public EntryPredicateFilter(BitSet fieldsToReturn, List<Predicate> predicates){
        this.fieldsToReturn = fieldsToReturn;
        this.valuePredicates = predicates;
    }

    public boolean match(EntryDecoder entry,EntryAccumulator accumulator) throws IOException {
        BitIndex encodedIndex = entry.getCurrentIndex();
        BitSet remainingFields = accumulator.getRemainingFields();

        MultiFieldDecoder decoder = entry.getEntryDecoder();
        for(int encodedPos =encodedIndex.nextSetBit(0);
            encodedPos>=0&&encodedPos<=remainingFields.length();
            encodedPos=encodedIndex.nextSetBit(encodedPos+1)){
            if(!remainingFields.get(encodedPos)){
                seekForward(encodedIndex,decoder,encodedPos,decoder.offset());
                continue;
            }

            int offset = decoder.offset();

            byte[] array = decoder.array();
            if(offset>array.length){
                /*
                 * It may seem odd that this case can happen, but it occurs when the scan
                 * doesn't provide information about how many columns it expects back, and
                 * the bit index is an AllFullBitIndex (This is the case when you want to return
                 * everything, and everything is present). In that case, there is nothing to stop
                 * this from looping off the end of the data array (which, in fact, is the correct
                 * behavior when you want to return everything), so we have to check and make sure that
                 * there actually IS data to add before adding. In that case, we've finished, so
                 * tell the accumulator
                 */
                if(accumulator instanceof AlwaysAcceptEntryAccumulator)
                    ((AlwaysAcceptEntryAccumulator)accumulator).complete();
                return true;
            }
            seekForward(encodedIndex, decoder, encodedPos, offset);


            int limit = decoder.offset()-1-offset;
            if(offset+limit>array.length){
                limit = array.length-offset;
            }
            for(Predicate valuePredicate : valuePredicates){
                if(!valuePredicate.match(encodedPos,array, offset,limit)){
                    return false;
                }
            }
            accumulator.add(encodedPos,ByteBuffer.wrap(array, offset,limit));
        }
        return true;
    }

    private void seekForward(BitIndex encodedIndex, MultiFieldDecoder decoder, int encodedPos, int offset) {
    /*
     * Certain fields may contain zeros--in particular, scalar, float, and double types. We need
     * to skip past those zeros without treating them as delimiters. Since we have that information
     * in the index, we can simply decode and throw away the proper type to adjust the offset properly.
     * However, in some cases it's more efficient to skip the count directly, since we may know the
     * byte size already.
     */
        if(encodedIndex.isScalarType(encodedPos)){
            decoder.decodeNextLong(); //don't need the value, just need to seek past it
        }else if(encodedIndex.isFloatType(encodedPos)){
            //floats are always 4 bytes, so skip the after delimiter
            decoder.seek(offset + 5);
        }else if(encodedIndex.isDoubleType(encodedPos)){
            //doubles are always 8 bytes, so skip the after delimiter as well
            decoder.seek(offset + 9);
        }else
            decoder.skip();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(fieldsToReturn);
        out.writeInt(valuePredicates.size());
        for (Predicate valuePredicate : valuePredicates) {
            out.writeObject(valuePredicate);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fieldsToReturn = (BitSet)in.readObject();
        int size = in.readInt();
        valuePredicates = Lists.newArrayListWithCapacity(size);
        for(int i=0;i<size;i++){
            valuePredicates.add((Predicate)in.readObject());
        }
    }

    public EntryAccumulator newAccumulator(){
        if(fieldsToReturn.isEmpty())
            return new AlwaysAcceptEntryAccumulator();
        else
            return new SparseEntryAccumulator(fieldsToReturn);
    }
}
