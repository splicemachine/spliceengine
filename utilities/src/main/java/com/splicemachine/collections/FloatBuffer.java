package com.splicemachine.collections;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 7/13/15
 */
public class FloatBuffer{
    protected float[] buffer;
    protected int size;

    public FloatBuffer(){
        this(10);
    }

    public FloatBuffer(int size){
        this.buffer = new float[size];
    }

    public void add(float l){
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

    public float[] toArray(){
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
