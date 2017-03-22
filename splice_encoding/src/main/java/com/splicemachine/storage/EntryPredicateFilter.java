/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Supplier;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;
/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class EntryPredicateFilter {
    public static final EntryPredicateFilter EMPTY_PREDICATE = new EntryPredicateFilter(new BitSet(), new ObjectArrayList<Predicate>());
    private BitSet fieldsToReturn;
    private ObjectArrayList<Predicate> valuePredicates;
    private boolean returnIndex;
    private BitSet predicateColumns;

		private long rowsFiltered = 0l;

		public static EntryPredicateFilter emptyPredicate(){ return EMPTY_PREDICATE; }

    public EntryPredicateFilter(BitSet fieldsToReturn, ObjectArrayList<Predicate> predicates){
        this(fieldsToReturn, predicates,true);
    }

    public EntryPredicateFilter(BitSet fieldsToReturn, ObjectArrayList<Predicate> predicates,boolean returnIndex){
        this.fieldsToReturn = fieldsToReturn;
        this.valuePredicates = predicates;
        this.returnIndex=returnIndex;
    }

		public boolean match(Indexed index,
												 Supplier<MultiFieldDecoder> decoderProvider,
												 EntryAccumulator accumulator) throws IOException{
				BitSet remainingFields = accumulator.getRemainingFields();

				MultiFieldDecoder decoder = decoderProvider.get();
				for(int encodedPos =index.nextSetBit(0);
						remainingFields.cardinality()>0 && encodedPos>=0&&encodedPos<=remainingFields.length();
						encodedPos=index.nextSetBit(encodedPos + 1)){
						if(!remainingFields.get(encodedPos)){
								skipField(decoder,encodedPos,index);
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
								if(fieldsToReturn==null||fieldsToReturn.isEmpty())
										accumulator.complete();
								return true;
						}
						skipField(decoder, encodedPos, index);


						int limit = decoder.offset()-1-offset;
						if(limit<=0){
								//we have an implicit null field
								limit=0;
						}else if(offset+limit>array.length){
								limit = array.length-offset;
						}

						Object[] buffer = valuePredicates.buffer;
						int bufferSize = valuePredicates.size();
						if(bufferSize>0){
								int predicatePosition = index.getPredicatePosition(encodedPos);
								for (int i =0; i<bufferSize; i++) {
										if(((Predicate)buffer[i]).applies(predicatePosition) && !((Predicate)buffer[i]).match(predicatePosition,array, offset,limit)){
												rowsFiltered++;
												return false;
										}
								}
						}

						accumulate(index, encodedPos, accumulator, array, offset, limit);
				}
				return true;
		}


		public boolean match(EntryDecoder entry,EntryAccumulator accumulator) throws IOException {
				return match(entry.getCurrentIndex(),entry, accumulator);
    }

    public BitSet getCheckedColumns(){
        if(predicateColumns==null){
            predicateColumns = new BitSet();
            Object[] buffer = valuePredicates.buffer;
            int ibuffer = valuePredicates.size();
            for (int i =0; i<ibuffer; i++) {
                ((Predicate)buffer[i]).setCheckedColumns(predicateColumns);
            }
        }
        return predicateColumns;
    }

    public boolean checkPredicates(ByteSlice buffer,int position){
        Object[] vpBuffer = valuePredicates.buffer;
        int ibuffer = valuePredicates.size();
        for (int i =0; i<ibuffer; i++) {
            Predicate predicate = (Predicate) vpBuffer[i];
            if(!predicate.applies(position))
                continue;
            if(predicate.checkAfter()){
                if(buffer!=null){
                    if(!predicate.match(position,buffer.array(),buffer.offset(),buffer.length()))
                        return false;
                }else{
                    if(!predicate.match(position,null,0,0))
                        return false;
                }
            }
        }
        return true;
    }

    public void rowReturned(){
        //no-op
    }

    public void reset(){
        Object[] vpBuffer = valuePredicates.buffer;
        int ibuffer = valuePredicates.size();
        for (int i =0; i<ibuffer; i++) {
        	((Predicate)vpBuffer[i]).reset();
        	
        }
    }

    public EntryAccumulator newAccumulator() {
        return new ByteEntryAccumulator(this,returnIndex,fieldsToReturn);
    }

    public long getRowsOutput() {
        return 0;
    }

		public long getRowsFiltered(){ return rowsFiltered; }

    public byte[] toBytes() {
        //if we dont have any distinguishing information, just send over an empty byte array
        if(fieldsToReturn.length()==0 && valuePredicates.size()<=0 && !returnIndex)
            return new byte[]{};

        /*
         * Format is as follows:
         * BitSet bytes
         * 1-byte returnIndex
         * n-bytes predicates
         */
        byte[] bitSetBytes = Bytes.toByteArray(fieldsToReturn);
        byte[] predicates = Predicates.toBytes(valuePredicates);
        int size = predicates.length+bitSetBytes.length+1;

        byte[] finalData = new byte[size];
        System.arraycopy(bitSetBytes,0,finalData,0,bitSetBytes.length);
        finalData[bitSetBytes.length] = returnIndex? (byte)0x01: 0x00;
        System.arraycopy(predicates,0,finalData,bitSetBytes.length+1,predicates.length);
//        int index = bitSetBytes.length + 1 + predicates.length;

        return finalData;
    }

    public static EntryPredicateFilter fromBytes(byte[] data) throws IOException {
        if(data==null||data.length==0) return EMPTY_PREDICATE;

        Pair<BitSet,Integer> fieldsToReturn = Bytes.fromByteArray(data, 0);
        boolean returnIndex = data[fieldsToReturn.getSecond()] > 0;
        Pair<ObjectArrayList<Predicate>,Integer> predicates = Predicates.allFromBytes(data,fieldsToReturn.getSecond()+1);
				return new EntryPredicateFilter(fieldsToReturn.getFirst(),predicates.getFirst(),returnIndex);
    }

    public ObjectArrayList<Predicate> getValuePredicates() {
        return valuePredicates;
    }

    public void setValuePredicates(ObjectArrayList<Predicate> valuePredicates) {
        this.valuePredicates = valuePredicates;
    }

    private void skipField(MultiFieldDecoder decoder, int position, Indexed index) {
				if(index.isScalarType(position)){
						decoder.skipLong();
				}else if(index.isFloatType(position)){
						decoder.skipFloat();
				}else if(index.isDoubleType(position))
						decoder.skipDouble();
				else
						decoder.skip();
		}

		private void accumulate(Indexed index, int position, EntryAccumulator accumulator, byte[] buffer, int offset, int length) {
				if(index.isScalarType(position)){
						accumulator.addScalar(position,buffer,offset,length);
				}else if(index.isFloatType(position)){
						accumulator.addFloat(position,buffer,offset,length);
				}else if(index.isDoubleType(position))
						accumulator.addDouble(position,buffer,offset,length);
				else
						accumulator.add(position,buffer,offset,length);
		}

    public boolean indexReturned() {
        return returnIndex;
    }
}
