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

package com.splicemachine.collections;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Scott Fines
 *         Date: 8/17/15
 */
public class Sample<T> extends AbstractCollection<T>{
    private final int maxSize;
    private final Random rng;

    private Object[] data;
    private int size;
    private int sampleSize = 0;

    public Sample(int maxSize,Random rng){
        this.maxSize=maxSize;
        this.rng=rng;
        this.data = new Object[10];
    }

    @Override
    public boolean add(T t){
        if(size<maxSize){
            place(t);
            sampleSize++;
            return true;
        }

        int n = rng.nextInt(sampleSize);
        sampleSize++;
        if(n<size){
            data[n] = t;
            return true;
        }
        return false;
    }

    @Override
    public void clear(){
        size = 0;
    }

    @Override
    public Iterator<T> iterator(){
        return new Itr();
    }

    @Override
    public int size(){
        return size;
    }

    /* **********************************************************************************************************/
    /*private helper methods and classes*/
    private class Itr implements Iterator<T>{
        private int pos =0;
        @Override
        public boolean hasNext(){
            return pos < size;
        }

        @Override
        public T next(){
            Object o =data[pos];
            pos++;
            //noinspection unchecked
            return (T)o;
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException("Removal not supported");
        }
    }

    private void place(T t){
        if(size==data.length)
            data  =Arrays.copyOf(data,Math.min(maxSize,2*size));
        data[size] = t;
        size++;
    }
}
