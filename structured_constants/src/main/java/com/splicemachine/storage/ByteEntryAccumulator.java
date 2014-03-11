package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 * Created on: 10/2/13
 */
public class ByteEntryAccumulator implements EntryAccumulator{

		protected EntryAccumulationSet accumulationSet;

    protected ByteSlice[] fields;
    private final boolean returnIndex;

    private EntryPredicateFilter predicateFilter;
		private long finishCount;

		public ByteEntryAccumulator(EntryPredicateFilter filter, boolean returnIndex, BitSet fieldsToCollect){
				this.returnIndex = returnIndex;
				this.predicateFilter = filter;
				if(fieldsToCollect!=null && !fieldsToCollect.isEmpty()){
						this.accumulationSet = new SparseAccumulationSet(fieldsToCollect);
						this.fields = new ByteSlice[(int)fieldsToCollect.length()];
				}else
						this.accumulationSet = new AlwaysAcceptAccumulationSet();
		}

		public ByteEntryAccumulator(EntryPredicateFilter filter, boolean returnIndex) {
				this(filter,returnIndex,null);
		}

		public ByteEntryAccumulator(EntryPredicateFilter filter, BitSet bitSet) {
				this(filter,false,bitSet);
		}

		@Override
		public void add(int position, byte[] data, int offset, int length) {
				if(occupy(position, data, offset, length))
						accumulationSet.addUntyped(position);
		}

		protected boolean occupy(int position, byte[] data, int offset, int length) {
				if(accumulationSet.get(position)) return false; //already populated that field

				growFields(position);
				ByteSlice slice = fields[position];
				if(slice==null){
						slice = ByteSlice.wrap(data,offset,length);
						fields[position] = slice;
				}else
					slice.set(data,offset,length);
				return true;
		}

		@Override
		public void addScalar(int position, byte[] data, int offset, int length) {
				if(occupy(position,data,offset,length))
						accumulationSet.addScalar(position);
		}

		@Override
		public void addFloat(int position, byte[] data, int offset, int length) {
				if(occupy(position,data,offset,length))
						accumulationSet.addFloat(position);
		}

		@Override
		public void addDouble(int position, byte[] data, int offset, int length) {
				if(occupy(position,data,offset,length))
						accumulationSet.addDouble(position);
		}

		@Override
		public BitSet getRemainingFields() {
				return accumulationSet.remainingFields();
		}

		@Override
		public boolean isFinished() {
				return accumulationSet.isFinished();
		}

		@Override
    public byte[] finish() {
				finishCount++;
        if(predicateFilter!=null){
            predicateFilter.reset();
            BitSet checkColumns = predicateFilter.getCheckedColumns();
            if(fields!=null){
                for(int i=checkColumns.nextSetBit(0);i>=0;i=checkColumns.nextSetBit(i+1)){
										boolean isNull = i>=fields.length || fields[i]==null || fields[i].length()<=0;
                    if(isNull){
                        if(!predicateFilter.checkPredicates(null,i)) return null;
                    }else{
                        ByteSlice buffer = fields[i];
                        if(!predicateFilter.checkPredicates(buffer,i)) return null;
                    }
                }
						}else{
								for(int i=0;i<checkColumns.length();i++){
										if(!predicateFilter.checkPredicates(null,i)) return null;
								}
            }

            predicateFilter.rowReturned();
        }

        byte[] dataBytes = getDataBytes();
        if(returnIndex){
						byte[] indexBytes = accumulationSet.encode();

            byte[] finalBytes = new byte[indexBytes.length+dataBytes.length+1];
            System.arraycopy(indexBytes,0,finalBytes,0,indexBytes.length);
            System.arraycopy(dataBytes,0,finalBytes,indexBytes.length+1,dataBytes.length);
            return finalBytes;
        }
        return dataBytes;
    }

//		protected byte[] getIndex() {
//				BitIndex index = BitIndexing.uncompressedBitMap(occupiedFields, scalarFields, floatFields, doubleFields);
//				return index.encode();
//		}

		@Override
    public void reset() {
				accumulationSet.reset();
        if(fields!=null){
						for (ByteSlice field : fields) {
								if (field != null)
										field.reset();
						}
				}

        if(predicateFilter!=null)
            predicateFilter.reset();
    }

    @Override
    public boolean hasField(int myFields) {
				return accumulationSet.get(myFields);
    }

		@Override
		public ByteSlice getFieldSlice(int myField) {
				ByteSlice field = fields[myField];
				if(field.length()<=0) return null;
				return field;
		}

		@Override
		public ByteSlice getField(int myField, boolean create) {
				ByteSlice field = fields[myField];
				if(field==null && create){
						field = ByteSlice.empty();
						fields[myField] = field;
				}
				return field;
		}

		protected byte[] getDataBytes() {
        int size=0;
        boolean isFirst=true;
				BitSet occupiedFields = accumulationSet.occupiedFields;
        for(int n = occupiedFields.nextSetBit(0);n>=0;n=occupiedFields.nextSetBit(n+1)){
            if(isFirst)isFirst=false;
            else
                size++;
            ByteSlice buffer = fields[n];
            if(buffer!=null){
                size+=buffer.length();
            }
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for(int n=occupiedFields.nextSetBit(0);n>=0;n=occupiedFields.nextSetBit(n+1)){
            if(isFirst)isFirst=false;
            else
                offset++;

            ByteSlice buffer = fields[n];
            if(buffer!=null){
                int newOffset = offset+buffer.length();
                buffer.get(bytes,offset);
                offset=newOffset;
            }
        }
        return bytes;
    }

    @Override
    public boolean fieldsMatch(EntryAccumulator oldKeyAccumulator) {
				BitSet occupiedFields = accumulationSet.occupiedFields;
        for(int myFields=occupiedFields.nextSetBit(0);myFields>=0;myFields=occupiedFields.nextSetBit(myFields+1)){
            if(!oldKeyAccumulator.hasField(myFields)) return false;

            ByteSlice myField = getFieldSlice(myFields);
            ByteSlice theirField = oldKeyAccumulator.getFieldSlice(myFields);
            if(myField==null){
                if(theirField!=null) return false;
            }else if(!myField.equals(theirField)) return false;
        }
        return true;
    }

		@Override public long getFinishCount() { return finishCount; }
		@Override public void markOccupiedScalar(int position) { accumulationSet.addScalar(position); }
		@Override public void markOccupiedFloat(int position) { accumulationSet.addFloat(position); }
		@Override public void markOccupiedDouble(int position) { accumulationSet.addDouble(position); }
		@Override public void markOccupiedUntyped(int position) { accumulationSet.addUntyped(position); }

		@Override public boolean isInteresting(BitIndex potentialIndex) {
				return accumulationSet.isInteresting(potentialIndex);
		}

		@Override public void complete() { accumulationSet.complete(); }

		private void growFields(int position) {
        /*
         * Make sure that the fields array is large enough to hold elements up to position.
         */
				if(fields==null){
						fields = new ByteSlice[position+1];
				}else if(fields.length<=position && !accumulationSet.isFinished()){ //if completed, we know how many to return
						//grow the fields list to be big enough to hold the position

            /*
             * In Normal circumstances, we would grow by some efficient factor
             * like 3/2 or something, so that we don't have to copy out entries over and over again.
             *
             * However, in this case, we can't grow past what we need, because that would place additional
             * null entries at the end of our return array. Instead, we must only be as large as needed
             * to hold position.
             *
             * This isn't so bad, though--once the first row has been resolved, we should never have
             * to grow again, so we'll pay a penalty on the first row only.
             */
						int newSize = position+1;
						ByteSlice[] oldFields = fields;
						fields = new ByteSlice[newSize];
						System.arraycopy(oldFields,0,fields,0,oldFields.length);
				}
		}

}
