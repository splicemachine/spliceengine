package com.splicemachine.utils;

/**
 * A ByteSlice that keeps an on-demand cached version of
 * a byte copy. This way, calling getByteCopy() will not create
 * a million copies if it's called a million times.
 *
 * <p>
 *    Of course, this strategy can be dangerous if you are modifying the underlying
 *    byte[] (since we are passing the same byte[] reference around). As a result,
 *    this is mainly useful in read-only views of data.
 * </p>
 *
 * @author Scott Fines
 *         Date: 1/23/15
 */
public class CachedByteSlice extends ByteSlice {
    private byte[] cachedCopy = null;

    public CachedByteSlice() { }

    public CachedByteSlice(byte[] array) {
        this(array,0,array.length);
    }

    public CachedByteSlice(ByteSlice other) {
        super(other);
    }

    public CachedByteSlice(byte[] array,int offset, int length) {
        super(array,offset,length);
    }

    @Override
    public byte[] getByteCopy() {
        if(cachedCopy==null)
            cachedCopy = super.getByteCopy();
        return cachedCopy;
    }

    @Override
    public void set(byte[] buffer, int offset, int length) {
        super.set(buffer, offset, length);
        cachedCopy=null; //dereference the cached copy to keep in sync
    }

    @Override
    public void set(ByteSlice rowSlice, boolean reverse) {
        super.set(rowSlice, reverse);
        cachedCopy=null; //dereference the cached copy to keep in sync
    }

    @Override
    public void set(ByteSlice newData) {
        super.set(newData);
        cachedCopy=null;
    }

    @Override
    public void set(byte[] bytes) {
        super.set(bytes);
        cachedCopy=null;
    }
}
