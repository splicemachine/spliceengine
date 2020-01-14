/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.util;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.util.BitSet;

/**
 * JBitSet is a wrapper class for BitSet.  It is a fixed length implementation
 * which can be extended via the grow() method.  It provides additional
 * methods to manipulate BitSets.
 * NOTE: JBitSet was driven by the (current and perceived) needs of the
 * optimizer, but placed in the util package since it is not specific to
 * query trees..
 * NOTE: java.util.BitSet is final, so we must provide a wrapper class
 * which includes a BitSet member in order to extend the functionality.
 * We want to make it look like JBitSet extends BitSet, so we need to
 * provide wrapper methods for all of BitSet's methods.
 */
public final class JBitSet{
    /* The BitSet that we'd like to extend */
    private final BitSet bitSet;
    /* Cache size() of bitSet, since accessed a lot */
    private int size;

    /**
     * Construct a JBitSet of the specified size.
     *
     * @param size The number of bits in the JBitSet.
     */
    public JBitSet(int size){
        bitSet=new BitSet(size);
        this.size=size;
    }

    /**
     * Construct a JBitSet with the specified bitSet.
     *
     * @param bitSet The BitSet.
     * @param size   The size of bitSet.
     *               NOTE: We need to specify the size since the size of a
     *               BitSet is not guaranteed to be the same as JBitSet.size().
     */
    private JBitSet(BitSet bitSet,int size){
        this.bitSet=bitSet;
        this.size=size;
    }

    /**
     * Set the BitSet to have the exact same bits set as the parameter's BitSet.
     *
     * @param sourceBitSet The JBitSet to copy.
     */
    public void setTo(JBitSet sourceBitSet){
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(size==sourceBitSet.size(),
                    "JBitSets are expected to be the same size");
        }
        /* High reuse solution */
        and(sourceBitSet);
        or(sourceBitSet);
    }

    /**
     * Test to see if one JBitSet contains another one of
     * the same size.
     *
     * @param jBitSet JBitSet that we want to know if it is
     *                a subset of current JBitSet
     * @return boolean    Whether or not jBitSet is a subset.
     */
    public boolean contains(JBitSet jBitSet){
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(size==jBitSet.size(),
                    "JBitSets are expected to be the same size");
        }
        for(int bitIndex=0;bitIndex<size;bitIndex++){
            if(jBitSet.bitSet.get(bitIndex) && !(bitSet.get(bitIndex))){
                return false;
            }
        }
        return true;
    }

    public boolean intersects(JBitSet other){
        return bitSet.intersects(other.bitSet);
    }

    /**
     * See of a JBitSet has exactly 1 bit set.
     *
     * @return boolean    Whether or not JBitSet has a single bit set.
     */
    public boolean hasSingleBitSet(){
        boolean found=false;

        for(int bitIndex=0;bitIndex<size;bitIndex++){
            if(bitSet.get(bitIndex)){
                if(found){
                    return false;
                }else{
                    found=true;
                }
            }
        }

        return found;
    }

    /**
     * Get the first set bit (starting at index 0) from a JBitSet.
     *
     * @return int    Index of first set bit, -1 if none set.
     */
    public int getFirstSetBit(){
        for(int bitIndex=0;bitIndex<size;bitIndex++){
            if(bitSet.get(bitIndex)){
                return bitIndex;
            }
        }

        return -1;
    }

    /**
     * Grow an existing JBitSet to the specified size.
     *
     * @param newSize The new size
     */
    public void grow(int newSize){
        if(SanityManager.DEBUG){
            SanityManager.ASSERT(newSize>size,
                    "New size is expected to be larger than current size");
        }

        size=newSize;

    }

    /**
     * Clear all of the bits in this JBitSet
     */
    public void clearAll(){
        for(int bitIndex=0;bitIndex<size;bitIndex++){
            if(bitSet.get(bitIndex)){
                bitSet.clear(bitIndex);
            }
        }
    }

    /* Wrapper methods for BitSet's methods */
    @Override
    public String toString(){
        return bitSet.toString();
    }

    @Override
    public boolean equals(Object obj){
        if(obj instanceof BitSet){
            return bitSet.equals(obj);
        }else
            return obj instanceof JBitSet && bitSet.equals(((JBitSet)obj).bitSet);
    }

    @Override
    public int hashCode(){
        return bitSet.hashCode();
    }

    @SuppressWarnings("CloneDoesntCallSuperClone")
    @Override
    public Object clone(){
        return new JBitSet((BitSet)bitSet.clone(),size);
    }

    public boolean get(int bitIndex){
        return bitSet.get(bitIndex);
    }

    public void set(int bitIndex){
        bitSet.set(bitIndex);
    }

    public void clear(int bitIndex){
        bitSet.clear(bitIndex);
    }

    public void and(JBitSet set){
        bitSet.and(set.bitSet);
    }

    public void or(JBitSet set){
        bitSet.or(set.bitSet);
    }

    public void xor(JBitSet set){
        bitSet.xor(set.bitSet);
    }

    public void andNot(JBitSet set) {
        bitSet.andNot(set.bitSet);
    }

    /**
     * Return the size of bitSet
     *
     * @return int    Size of bitSet
     */
    public int size(){
        return size;
    }

    public int cardinality() {
        return bitSet.cardinality();
    }
}

