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

package com.splicemachine.collections;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 7/13/15
 */
public class DoubleBuffer{
    protected double[] buffer;
    protected int size;

    public DoubleBuffer(){
        this(10);
    }

    public DoubleBuffer(int size){
        this.buffer = new double[size];
    }

    public void add(double l){
        ensureSpace();
        buffer[size] = l;
        size++;
    }

    public double get(int pos){
        if(pos<0||pos>=size){
            throw new ArrayIndexOutOfBoundsException();
        }
        return buffer[pos];
    }

    public double[] toArray(){
        return Arrays.copyOf(buffer,size);
    }

    public int size(){
        return size;
    }

    public void clear(){
        size = 0;
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private void ensureSpace(){
        if(size==buffer.length){
            expand();
        }
    }

        /*Stolen from ArrayList*/
    /**
     * The maximum size of array to allocate.
     * Some VMs reserve some header words in an array.
     * Attempts to allocate larger arrays may result in
     * OutOfMemoryError: Requested array size exceeds VM limit
     */
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    private void expand() {
        // overflow-conscious code
        int minCapacity = buffer.length+1;
        int oldCapacity = buffer.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
        if (newCapacity - MAX_ARRAY_SIZE > 0)
            newCapacity = hugeCapacity(minCapacity);
        // minCapacity is usually close to size, so this is a win:
        buffer = Arrays.copyOf(buffer,newCapacity);
    }

    private static int hugeCapacity(int minCapacity) {
        if (minCapacity < 0) // overflow
            throw new OutOfMemoryError();
        return (minCapacity > MAX_ARRAY_SIZE) ?
                Integer.MAX_VALUE :
                MAX_ARRAY_SIZE;
    }
}
