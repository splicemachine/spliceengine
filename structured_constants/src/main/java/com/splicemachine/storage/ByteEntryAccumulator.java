package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 * Created on: 10/2/13
 */
public class ByteEntryAccumulator extends BaseEntryAccumulator<ByteEntryAccumulator>{

    protected ByteSlice[] fields;


		public ByteEntryAccumulator(EntryPredicateFilter filter, boolean returnIndex, BitSet fieldsToCollect){
				super(filter,returnIndex,fieldsToCollect);
				if(fieldsToCollect!=null && !fieldsToCollect.isEmpty())
						this.fields = new ByteSlice[(int)fieldsToCollect.length()];
		}

		public ByteEntryAccumulator(EntryPredicateFilter filter, boolean returnIndex) {
				this(filter,returnIndex,null);
		}

		public ByteEntryAccumulator(EntryPredicateFilter filter, BitSet bitSet) {
				this(filter,false,bitSet);
		}


		protected void occupy(int position, byte[] data, int offset, int length) {
				growFields(position);
				ByteSlice slice = fields[position];
				if(slice==null){
						slice = ByteSlice.wrap(data,offset,length);
						fields[position] = slice;
				}else
					slice.set(data,offset,length);
		}


		@Override
    public byte[] finish() {
				finishCount++;
				if (checkFilterAfter()) return null;

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

		protected boolean checkFilterAfter() {
				if(predicateFilter!=null){
						predicateFilter.reset();
						BitSet checkColumns = predicateFilter.getCheckedColumns();
						if(fields!=null){
								for(int i=checkColumns.nextSetBit(0);i>=0;i=checkColumns.nextSetBit(i+1)){
										if(!accumulationSet.get(i)) continue; //we aren't interested in this field
										boolean isNull = i>=fields.length || fields[i]==null || fields[i].length()<=0;
										if(isNull){
												if(!predicateFilter.checkPredicates(null,i)) return true;
										}else{
												ByteSlice buffer = fields[i];
												if(!predicateFilter.checkPredicates(buffer,i)) return true;
										}
								}
						}else{
								for(int i=0;i<checkColumns.length();i++){
										if(!accumulationSet.get(i)) continue; //ignore fields that we aren't interested in
										if(!predicateFilter.checkPredicates(null,i)) return true;
								}
						}

						predicateFilter.rowReturned();
				}
				return false;
		}


		@Override
    public void reset() {
				super.reset();
        if(fields!=null){
						for (ByteSlice field : fields) {
								if (field != null)
										field.reset();
						}
				}

    }

		public ByteSlice getFieldSlice(int myField) {
				ByteSlice field = fields[myField];
				if(field.length()<=0) return null;
				return field;
		}

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
		protected boolean matchField(int myFields,ByteEntryAccumulator otherAccumulator) {
				ByteSlice myField = getFieldSlice(myFields);
				ByteSlice theirField = otherAccumulator.getFieldSlice(myFields);
				return myField == null ? theirField == null : myField.equals(theirField);
		}

		private void growFields(int position) {
        /*
         * Make sure that the fields array is large enough to hold elements up to position.
         */
				if(fields==null){
						fields = new ByteSlice[position+1];
				}else if(fields.length<=position){ //if completed, we know how many to return
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

		@Override
		protected void occupyDouble(int position, byte[] data, int offset,
				int length) {
			shortCircuitOccupy(position, data, offset, length);			
		}

		@Override
		protected void occupyFloat(int position, byte[] data, int offset,
				int length) {
			shortCircuitOccupy(position, data, offset, length);		
		}

		@Override
		protected void occupyScalar(int position, byte[] data, int offset,
				int length) {
			shortCircuitOccupy(position, data, offset, length);						
		}
		
		protected void shortCircuitOccupy(int position, byte[] data, int offset, int length) {
			growFields(position);
			ByteSlice slice = fields[position];
			if(slice==null){
					slice = ByteSlice.wrap(data,offset,length);
					fields[position] = slice;
			}else
				slice.set(data,offset,length);
	}

}
