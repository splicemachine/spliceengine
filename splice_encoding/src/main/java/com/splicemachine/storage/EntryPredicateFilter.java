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

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;
import splice.com.google.common.base.Supplier;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class EntryPredicateFilter {
    public static final EntryPredicateFilter EMPTY_PREDICATE = new EntryPredicateFilter(new BitSet());
    private BitSet fieldsToReturn;
    private boolean returnIndex;
    public static EntryPredicateFilter emptyPredicate(){ return EMPTY_PREDICATE; }

    public EntryPredicateFilter(BitSet fieldsToReturn){
        this(fieldsToReturn, true);
    }

    public EntryPredicateFilter(BitSet fieldsToReturn, boolean returnIndex){
        this.fieldsToReturn = fieldsToReturn;
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
						accumulate(index, encodedPos, accumulator, array, offset, limit);
				}
				return true;
		}


		public boolean match(EntryDecoder entry,EntryAccumulator accumulator) throws IOException {
				return match(entry.getCurrentIndex(),entry, accumulator);
    }

    public void rowReturned(){
        //no-op
    }

    public void reset(){

    }

    public EntryAccumulator newAccumulator() {
        return new ByteEntryAccumulator(this,returnIndex,fieldsToReturn);
    }

    public byte[] toBytes() {
        //if we dont have any distinguishing information, just send over an empty byte array
        if(fieldsToReturn.isEmpty() && !returnIndex)
            return new byte[]{};

        /*
         * Format is as follows:
         * BitSet bytes
         * 1-byte returnIndex
         */
        byte[] bitSetBytes = Bytes.toByteArray(fieldsToReturn);
        byte[] finalData = new byte[bitSetBytes.length+1];
        System.arraycopy(bitSetBytes,0,finalData,0,bitSetBytes.length);
        finalData[bitSetBytes.length] = returnIndex? (byte)0x01: 0x00;
        return finalData;
    }

    public static EntryPredicateFilter fromBytes(byte[] data) throws IOException {
        if(data==null||data.length==0) return EMPTY_PREDICATE;

        Pair<BitSet,Integer> fieldsToReturn = Bytes.fromByteArray(data, 0);
        boolean returnIndex = data[fieldsToReturn.getSecond()] > 0;
        return new EntryPredicateFilter(fieldsToReturn.getFirst(),returnIndex);
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
