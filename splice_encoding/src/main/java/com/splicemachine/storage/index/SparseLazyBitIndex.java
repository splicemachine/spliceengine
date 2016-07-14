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

package com.splicemachine.storage.index;

/**
 * Lazy implementation of a Sparse Bit Index, which decodes values as needed.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class SparseLazyBitIndex extends LazyBitIndex{

    int lastPos=0;
    protected SparseLazyBitIndex(byte[] encodedBitMap, int offset, int length) {
        super(encodedBitMap, offset, length,5);

        if(bitReader.hasNext()&&bitReader.next()!=0){
            decodedBits.set(0); //check the zero bit
            getNextType(0);
        }
    }


    @Override
    protected int decodeNext() {
        int val = DeltaCoding.decode(bitReader);
        if(val<0)
            return val;

        int pos = lastPos+val;
        getNextType(pos);

        lastPos=pos;
        return pos;
    }

    private void getNextType(int pos) {
    /*
     * Determine type information:
     *
     * Untyped: 00
     * Double: 01
     * Float: 10
     * Scalar: 11
     */
        if(!bitReader.hasNext()){
            //type information truncated, assume untyped
            return;
        }
        if(bitReader.next()!=0){
            //either scalar or float
            if(!bitReader.hasNext()){
                //type information truncated, assume float
                setFloatField(pos);
                return;
            }

            if(bitReader.next()!=0)
                setScalarField(pos);
            else
                setFloatField(pos);
        }else{
            //either double or untyped
            if(!bitReader.hasNext()){
                //type information truncated -- assume untyped
                return;
            }

            if(bitReader.next()!=0){
                setDoubleField(pos);
            }
        }
    }


	@Override
	public boolean isCompressed() {
		return false;
	}
}
