/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

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

    public int getCurrentLength(int position){
        if(position<0 || position>=fields.length) return 0;
        ByteSlice slice = fields[position];
        if(slice==null) return 0;
        return slice.length();
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
    protected void occupyDouble(int position, byte[] data, int offset, int length) {
        shortCircuitOccupy(position, data, offset, length);
    }

    @Override
    protected void occupyFloat(int position, byte[] data, int offset, int length) {
        shortCircuitOccupy(position, data, offset, length);
    }

    @Override
    protected void occupyScalar(int position, byte[] data, int offset, int length) {
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
