package com.splicemachine.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
@SuppressFBWarnings(value = "EQ_DOESNT_OVERRIDE_EQUALS",justification = "Intentional")
public class CachedByteSlice extends ByteSlice {
    private transient byte[] cachedCopy = null;

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
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
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

    @Override
    public void reset() {
        cachedCopy = null;
        super.reset();
    }

    @Override
    public void reverse() {
        cachedCopy = null;
        super.reverse();
    }

    @Override
    public String toString() {
        return super.toString() + " cachedCopy.length=" + (cachedCopy == null ? 0 : cachedCopy.length);
    }

    @Override
    @SuppressFBWarnings(value = "CN_IDIOM_NO_SUPER_CALL",justification = "Intentional")
    @SuppressWarnings("CloneDoesntCallSuperClone") //intentionally doesn't call it
    public ByteSlice clone(){
        if(array()==null) return new CachedByteSlice();
        if(cachedCopy!=null)
            return new CachedByteSlice(cachedCopy,0,cachedCopy.length);
        return new CachedByteSlice(getByteCopy(),0,length());
    }
}
