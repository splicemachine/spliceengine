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

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.BitReader;


/**
 * BitIndex which lazily decodes entries as needed, and which does not re-encode entries.
 *
 * @author Scott Fines
 *         Created on: 7/8/13
 */
public abstract class LazyBitIndex implements BitIndex{
    protected BitSet decodedBits;
    protected BitSet decodedScalarFields;
    protected BitSet decodedFloatFields;
    protected BitSet decodedDoubleFields;
    protected byte[] encodedBitMap;
    protected int offset;
    protected int length;
    protected BitReader bitReader;


    protected LazyBitIndex(byte[] encodedBitMap,int offset,int length,int bitPos){
        this.encodedBitMap=encodedBitMap;
        this.offset=offset;
        this.length=length;
        this.decodedBits=new BitSet();
        this.bitReader=new BitReader(encodedBitMap,offset,length,bitPos);
    }

    @Override
    public int length(){
        decodeAll();
        return (int)decodedBits.length();
    }

    private void decodeAll(){
        int next;
        while((next=decodeNext())>=0){
            decodedBits.set(next);
        }
    }

    protected abstract int decodeNext();

    @Override
    public boolean isSet(int pos){
        decodeUntil(pos);
        return decodedBits.get(pos);
    }

    public void decodeUntil(int pos){
        while(decodedBits.length()<pos+1){
            int next=decodeNext();
            if(next<0) return; //out of data to decode

            decodedBits.set(next);
        }
    }

    @Override
    public String toString(){
        return "{"+
                decodedBits+
                ","+decodedScalarFields+
                ","+decodedFloatFields+
                ","+decodedDoubleFields+
                '}';
    }

    @Override
    public int nextSetBit(int position){
        //try and get the next item out of decodedBits first
        int val=decodedBits.nextSetBit(position);
        if(val>=0) return val;

        //try decoding some more
        int i;
        do{
            i=decodeNext();
            if(i<0) break;

            decodedBits.set(i);
            if(i>=position)
                return i;
        }while(i>=0);

        //couldn't find any entries
        return -1;
    }

    @Override
    public int cardinality(){
        decodeAll();
        return (int)decodedBits.cardinality();
    }

    @Override
    public int cardinality(int position){
        decodeUntil(position);
        int count=0;
        for(int i=decodedBits.nextSetBit(0);i>=0 && i<position;i=decodedBits.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public boolean intersects(BitSet bitSet){
        decodeUntil((int)bitSet.length());
        return decodedBits.intersects(bitSet);
    }

    @Override
    public boolean isEmpty(){
        decodeAll();
        return decodedBits.isEmpty();
    }

    @Override
    public BitSet and(BitSet bitSet){
        decodeUntil((int)bitSet.length());
        final BitSet bits=(BitSet)decodedBits.clone();
        bits.and(bitSet);
        return bits;
    }

    @Override
    public byte[] encode(){
        byte[] copy=new byte[length];
        System.arraycopy(encodedBitMap,offset,copy,0,copy.length);
        return copy;
    }

    @Override
    public int encodedSize(){
        return length;
    }


    public boolean isScalarType(int position,boolean decoded){
        if(!decoded)
            decodeUntil(position);
        return decodedScalarFields!=null && decodedScalarFields.get(position);
    }

    public boolean isDoubleType(int position,boolean decoded){
        if(!decoded)
            decodeUntil(position);
        return decodedDoubleFields!=null && decodedDoubleFields.get(position);
    }

    public boolean isFloatType(int position,boolean decoded){
        if(!decoded)
            decodeUntil(position);
        return decodedFloatFields!=null && decodedFloatFields.get(position);
    }

    @Override
    public boolean isScalarType(int position){
        return isScalarType(position,false);
    }

    @Override
    public boolean isDoubleType(int position){
        return isDoubleType(position,false);
    }

    @Override
    public boolean isFloatType(int position){
        return isFloatType(position,false);
    }

    @Override
    public BitSet getScalarFields(){
        decodeAll();
        return decodedScalarFields;
    }

    @Override
    public BitSet getDoubleFields(){
        decodeAll();
        return decodedFloatFields;
    }

    @Override
    public BitSet getFloatFields(){
        decodeAll();
        return decodedDoubleFields;
    }


    protected void setScalarField(int pos){
        if(decodedScalarFields==null)
            decodedScalarFields=new BitSet(pos);
        decodedScalarFields.set(pos);
    }

    protected void setDoubleField(int pos){
        if(decodedDoubleFields==null)
            decodedDoubleFields=new BitSet(pos);
        decodedDoubleFields.set(pos);
    }

    protected void setFloatField(int pos){
        if(decodedFloatFields==null)
            decodedFloatFields=new BitSet(pos);
        decodedFloatFields.set(pos);
    }

    protected void setDoubleRange(int startPos,int stopPos){
        if(decodedDoubleFields==null)
            decodedDoubleFields=new BitSet(stopPos);
        decodedDoubleFields.set(startPos,stopPos);
    }

    protected void setFloatRange(int startPos,int stopPos){
        if(decodedFloatFields==null)
            decodedFloatFields=new BitSet(stopPos);
        decodedFloatFields.set(startPos,stopPos);
    }

    protected void setScalarRange(int startPos,int stopPos){
        if(decodedScalarFields==null)
            decodedScalarFields=new BitSet(stopPos);
        decodedScalarFields.set(startPos,stopPos);
    }

    public abstract boolean isCompressed();

}

