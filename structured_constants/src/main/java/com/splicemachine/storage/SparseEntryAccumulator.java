package com.splicemachine.storage;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class SparseEntryAccumulator extends GenericEntryAccumulator {
    private BitSet remainingFields;
    private BitSet allFields;

    public SparseEntryAccumulator(EntryPredicateFilter predicateFilter,BitSet remainingFields) {
        super(predicateFilter,remainingFields.length(),false);
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
    }

    public SparseEntryAccumulator(EntryPredicateFilter predicateFilter,BitSet remainingFields,boolean returnIndex) {
        super(predicateFilter,remainingFields.size(),returnIndex);
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

//    @Override
//    public boolean fieldsMatch(EntryAccumulator oldKeyAccumulator) {
//        for(int myFields=allFields.nextSetBit(0);myFields>=0;myFields=allFields.nextSetBit(myFields+1)){
//            if(!oldKeyAccumulator.hasField(myFields)) return false;
//            ByteBuffer field = fields[myFields];
//            if(field==null){
//                if(oldKeyAccumulator.getField(myFields)!=null) return false;
//            } else if(!field.equals(oldKeyAccumulator.getField(myFields))) return false;
//        }
//        return true;
//    }
}
