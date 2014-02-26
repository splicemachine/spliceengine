package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 * Created on: 10/2/13
 */
abstract class GenericEntryAccumulator implements EntryAccumulator{

    protected BitSet occupiedFields;

    protected ByteSlice[] fields;
    private final boolean returnIndex;

    private BitSet scalarFields;
    private BitSet floatFields;
    private BitSet doubleFields;
    private EntryPredicateFilter predicateFilter;
		private long finishCount;

    protected GenericEntryAccumulator(EntryPredicateFilter filter,boolean returnIndex) {
        this.returnIndex = returnIndex;
        this.predicateFilter =filter;
        
        if(returnIndex){
            scalarFields = new BitSet();
            floatFields = new BitSet();
            doubleFields = new BitSet();
        }
        occupiedFields = new BitSet();
    }

    protected GenericEntryAccumulator(EntryPredicateFilter filter,int size,boolean returnIndex) {
        this.returnIndex = returnIndex;

        this.predicateFilter = filter;
        if(returnIndex){
            scalarFields = new BitSet(size);
            floatFields = new BitSet(size);
            doubleFields = new BitSet(size);
        }
        occupiedFields = new BitSet(size);
        fields = new ByteSlice[size];
    }

		@Override
		public void add(int position, byte[] data, int offset, int length) {
				if(occupiedFields.get(position)) return; //already populated that field

				ByteSlice slice = fields[position];
				if(slice==null){
						slice = ByteSlice.wrap(data,offset,length);
						fields[position] = slice;
				}else
					slice.set(data,offset,length);
				occupiedFields.set(position);
		}

		@Override
		public void addScalar(int position, byte[] data, int offset, int length) {
			add(position,data,offset,length);
				if(returnIndex)
						scalarFields.set(position);
		}

		@Override
		public void addFloat(int position, byte[] data, int offset, int length) {
				add(position,data,offset,length);
				if(returnIndex)
						floatFields.set(position);
		}

		@Override
		public void addDouble(int position, byte[] data, int offset, int length) {
				add(position, data, offset, length);
				if(returnIndex)
						doubleFields.set(position);
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
						byte[] indexBytes = getIndex();

            byte[] finalBytes = new byte[indexBytes.length+dataBytes.length+1];
            System.arraycopy(indexBytes,0,finalBytes,0,indexBytes.length);
            System.arraycopy(dataBytes,0,finalBytes,indexBytes.length+1,dataBytes.length);
            return finalBytes;
        }
        return dataBytes;
    }

		protected byte[] getIndex() {
				BitIndex index = BitIndexing.uncompressedBitMap(occupiedFields, scalarFields, floatFields, doubleFields);
				return index.encode();
		}

		@Override
    public void reset() {
        occupiedFields.clear();
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
        return occupiedFields.get(myFields);
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

		@Override
		public void markOccupiedScalar(int position) {
				markOccupiedUntyped(position);
				if(returnIndex)
						scalarFields.set(position);
		}

		@Override
		public void markOccupiedFloat(int position) {
				markOccupiedUntyped(position);
				if(returnIndex)
						floatFields.set(position);
		}

		@Override
		public void markOccupiedDouble(int position) {
				markOccupiedUntyped(position);
				if(returnIndex)
						doubleFields.set(position);
		}

		@Override
		public void markOccupiedUntyped(int position) {
				occupiedFields.set(position);
		}
}
