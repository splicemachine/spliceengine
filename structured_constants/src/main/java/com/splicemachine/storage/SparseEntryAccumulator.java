package com.splicemachine.storage;

import java.nio.ByteBuffer;
import com.carrotsearch.hppc.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class SparseEntryAccumulator extends GenericEntryAccumulator {
    private BitSet remainingFields;
    private BitSet allFields;

    public SparseEntryAccumulator(EntryPredicateFilter predicateFilter,BitSet remainingFields) {
        super(predicateFilter,(int)remainingFields.length(),false);
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
    }

    public SparseEntryAccumulator(EntryPredicateFilter predicateFilter,BitSet remainingFields,boolean returnIndex) {
        super(predicateFilter,(int)remainingFields.size(),returnIndex);
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
    }

    @Override
    public void add(int position, ByteBuffer buffer){
        super.add(position,buffer);
        remainingFields.clear(position);
    }

    @Override
    public void addScalar(int position, ByteBuffer buffer) {
        super.addScalar(position,buffer);
        remainingFields.clear(position);
    }

    @Override
    public void addFloat(int position, ByteBuffer buffer) {
        super.addFloat(position,buffer);
        remainingFields.clear(position);
    }

    @Override
    public void addDouble(int position, ByteBuffer buffer) {
        super.addDouble(position, buffer);
        remainingFields.clear(position);
    }

    @Override
    public BitSet getRemainingFields(){
        return remainingFields;
    }

    @Override
    public void reset(){
        super.reset();
        remainingFields = (BitSet)allFields.clone();
    }
}
