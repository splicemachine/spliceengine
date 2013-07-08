package com.splicemachine.storage;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.index.BitIndex;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class EntryPredicateFilter implements Externalizable{
    private BitSet fieldsToReturn;
    private Predicate[] valuePredicates;

    /**
     * Used for Serialization, DO NOT USE
     */
    @Deprecated
    public EntryPredicateFilter(){}

    public EntryPredicateFilter(BitSet fieldsToReturn){
        this.fieldsToReturn = fieldsToReturn;
    }

    public boolean match(EntryDecoder entry,EntryAccumulator accumulator) throws IOException {
        BitIndex encodedIndex = entry.getCurrentIndex();
        BitSet remainingFields = accumulator.getRemainingFields();

        MultiFieldDecoder decoder = entry.getEntryDecoder();
        for(int encodedPos =encodedIndex.nextSetBit(0);
            encodedPos>=0&&encodedPos<=remainingFields.length();
            encodedPos=encodedIndex.nextSetBit(encodedPos+1)){
            if(!remainingFields.get(encodedPos)){
                decoder.skip();
                continue;
            }

            int offset = decoder.offset();
            decoder.skip();
            int limit = decoder.offset()-offset;

            for(Predicate valuePredicate : valuePredicates){
                if(valuePredicate.getColumn()==encodedPos){
                    if(!valuePredicate.match(decoder.array(), offset,limit)){
                        return false;
                    }
                }
            }
            accumulator.add(encodedPos,ByteBuffer.wrap(decoder.array(), offset,limit));
        }
        return true;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(fieldsToReturn);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fieldsToReturn = (BitSet)in.readObject();
    }

    public EntryAccumulator newAccumulator(){
        return new EntryAccumulator(fieldsToReturn);
    }
}
