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

package com.splicemachine.derby.stream.utils;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A list of booleans.
 *
 * @author Scott Fines
 *         Date: 9/29/16
 */
public class BooleanList extends AbstractList<Boolean>{
    private boolean[] list;
    private int size;

    public BooleanList(){
        this(10);
    }

    public BooleanList(int initialSize){
        this.list = new boolean[initialSize];
        size = 0;
    }

    public BooleanList(BooleanList bl){
        this.list = Arrays.copyOf(bl.list,bl.list.length);
        this.size = bl.size;
    }

    private BooleanList(boolean[] array){
        this.list = array;
        this.size= array.length;
    }

    @Override
    public boolean add(Boolean aBoolean){
        return append(aBoolean);
    }

    @Override
    public void add(int index,Boolean element){
        appendAt(index,element);
    }

    public boolean append(boolean bool){
        if(expandIfNecessary()) return false;

        list[size]=bool;
        size++;
        return true;
    }

    public void appendAt(int index,boolean element){
        if(index>=size) {
            append(element);
            return;
        }else if(index<0) throw new IndexOutOfBoundsException(Integer.toString(index));

        if(!expandIfNecessary()) throw new IndexOutOfBoundsException("List is too large!");
        System.arraycopy(list,index,list,index+1,size-index);
        list[index] = element;
        size++;
    }

    public boolean valueAt(int index){
        if(index<0 || index>=size) throw new IndexOutOfBoundsException(Integer.toString(index));
        return list[index];
    }

    @Override
    public void clear(){
        size = 0;
    }

    @Override
    public Boolean get(int index){
        return valueAt(index);
    }

    @Override
    public int size(){
        return size;
    }

    @Override
    public Iterator<Boolean> iterator(){
        return new BoolIter();
    }

    public BoolIter primitiveIterator(){
        return new BoolIter();
    }

    @Override
    public boolean equals(Object o){
        if(o==this) return true;
        if(!(o instanceof BooleanList)) return false;

        BooleanList other = (BooleanList)o;
        if(other.size!=size) return false;
        for(int i=0;i<size;i++){
            if(other.list[i]!=list[i]) return false;
        }
        return true;
    }

    @Override
    public int hashCode(){
        int hC = 17;
        for(int i=0;i<size;i++){
            hC=31*hC +(list[i]?1:0);
        }
        return hC;
    }

    public static BooleanList wrap(boolean... array){
        return new BooleanList(Arrays.copyOf(array,array.length));
    }

    public void trimToSize(){
        list = Arrays.copyOf(list,size);
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private boolean expandIfNecessary(){
        if(size>=list.length){
            int newSize = Math.min(Integer.MAX_VALUE,3*size/2);
            if(newSize!=list.length)
                list = Arrays.copyOf(list,newSize);
            else return true;
        }
        return false;
    }

    public class BoolIter implements Iterator<Boolean>{
        private int pos = 0;

        @Override
        public boolean hasNext(){
            return pos<size;
        }

        @Override
        public Boolean next(){
            return nextBoolean();
        }

        public boolean nextBoolean(){
            if(!hasNext()) throw new NoSuchElementException();
            boolean b = list[pos];
            pos++;
            return b;
        }
    }
}
