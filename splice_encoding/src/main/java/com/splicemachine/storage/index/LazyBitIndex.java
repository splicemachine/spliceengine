/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

