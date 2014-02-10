package com.splicemachine.storage;

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
		public void add(int position, byte[] data, int offset, int length) {
				super.add(position, data, offset, length);
				remainingFields.clear(position);
		}

    @Override
    public BitSet getRemainingFields(){
        return remainingFields;
    }

		@Override
		public boolean isFinished() {
				return remainingFields.cardinality()<=0;
		}

		@Override
    public void reset(){
        super.reset();
        remainingFields = (BitSet)allFields.clone();
    }
}
