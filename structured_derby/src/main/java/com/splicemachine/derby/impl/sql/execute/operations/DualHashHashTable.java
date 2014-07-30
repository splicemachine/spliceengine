package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.utils.RingBuffer;


/**
 * A Hashtable which has two mechanisms for hashing: an entry hash and a lookup hash.
 *
 * The Entry hash is used when writing data into the Hashtable, while the lookup hash
 * is used when attempting lookups against the hashtable. This way we can insert data
 * using a different hashing strategy than when we perform lookups, but still have matching hash
 * values (e.g. for left-and-right side joins of ExecRows, when the hash keys on the right
 * are different from those of the left, but the hashcodes should be equal).
 *
 * This implementation is a multi-hashtable--that is, multiple entries which are equal
 * <em> on their hash code</em> will be stored together. Entries which are <em>not</em>
 * the same on their hash codes will use linear probing to resolve hash conflicts.
 *
 * This class is <em>not</em> thread-safe; external synchronization must be used
 * if so desired (although it's not recommended for CPU cache reasons).
 *
 * @author Scott Fines
 * Date: 7/22/14
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class DualHashHashTable<T> {

    /**
     * Interface for defining a custom hash function.
     * @param <T> the element to be hashed.
     */
    public static interface EntryHasher<T>{
        /**
         * @param element the element to hash.
         * @return the hash code for the element.
         */
        int hash(T element);

        /**
         * Determine whether or not two entries are equal on their hash
         * code elements or not. We do not use raw equals() here, because it's
         * possible that some elements are hashed using only a subset of their equals()
         * fields.
         *
         * @param left the left item to hash (always the row that is passed in to the hashtable)
         * @param right the right item to hash (always the row that currently exists in the hashtable).
         * @return true if {@code left} shares the same hashcode fields as {@code right}
         */
        boolean equalsOnHash(T left, T right);
    }

    private RingBuffer<T>[] buffer;
    private int size;

    @SuppressWarnings("FieldCanBeLocal")
    private float fillFactor = 0.75f;
    private final EntryHasher<T> entryHasher;
    private final EntryHasher<T> lookupHasher;

    @SuppressWarnings("unchecked")
    public DualHashHashTable(int size,
                             EntryHasher<T> entryHasher,
                             EntryHasher<T> lookupHasher) {
        int s = 1;
        while(s<size)
            s<<=1;
        buffer = new RingBuffer[s];
        this.entryHasher = entryHasher;
        this.lookupHasher = lookupHasher;
    }

    /**
     * mark all buffers as at a read point--this allows them to be individually readReset
     * without incurring correctness penalties.
     */
    public void markAllBuffers() {
        for(int i=0;i<buffer.length;i++){
            RingBuffer<T> buf = buffer[i];
            if(buf!=null)
                buf.mark();
        }
    }

    /**
     * Add a new entry to the hashtable.
     * @param t the entry to add
     */
    public void add(T t){
        int slot = entryHasher.hash(t);
        int pos = slot & (buffer.length-1);

        int i;
        for(i=0;i<buffer.length;i++){
            RingBuffer<T> list = buffer[pos];
            if(list==null){
                list = new RingBuffer<T>(1);
                buffer[pos] = list;
                list.add(t);
                incrementSize(entryHasher);
                return;
            } else if(list.size()<=0){
                list.add(t);
                return;
            }else if(entryHasher.equalsOnHash(t,list.peek())){
                    if(list.isFull()){
                        list.expand();
                    }
                    list.add(t);
                    return;
            }
            pos = (pos+1) & (buffer.length-1);
        }
        //we couldn't find any room in the table, so expand the size and then try again
        resizeTable(entryHasher);
        add(t);
    }

    /**
     * Get data for the specified value.
     *
     * @param t the entry to get data for
     * @return all data for the specified element, or {@code null} if no
     * entry exists.
     */
    public RingBuffer<T> get(T t){
        int pos = lookupHasher.hash(t) & (buffer.length-1);
        for(int i=0;i<buffer.length;i++){
            RingBuffer<T> lookup = buffer[pos];
            if(lookup==null) return null; //we didn't find an entry in the table

            if(lookup.size() > 0){
                T first = lookup.peek();
                if(lookupHasher.equalsOnHash(t,first))
                    return lookup;
            }
            pos = (pos+1) & (buffer.length-1);
        }
        return null;
    }

    public int size(){
        return size;
    }

    /**
     * Clear the hashtable of existing elements
     */
    @SuppressWarnings("unchecked")
    public void clear(){
        //TODO -sf- is this cheaper than using Garbage Collection? It avoids excessive objects, but might not be ideal
        for(int i=0;i<buffer.length;i++){
            RingBuffer<T> rBuffer = buffer[i];
            if(rBuffer!=null)
                rBuffer.clear();
        }
        size = 0;
    }

    /**************************************************************************************************************/
    /*private helper methods*/
    private void incrementSize(EntryHasher<T> hasher) {
        size++;
        if(size>=fillFactor*buffer.length){
            resizeTable(hasher);
        }
    }

    @SuppressWarnings("unchecked")
    private void resizeTable(EntryHasher<T> hasher) {
        RingBuffer<T>[] newBuffer = new RingBuffer[2*buffer.length];
        for(int i=0;i<buffer.length;i++){
            RingBuffer<T> list = buffer[i];
            if(list==null) continue;
            put(newBuffer,list,hasher);
        }
        buffer = newBuffer;
    }

    private static <T> void put(RingBuffer<T>[] buf, RingBuffer<T> list, EntryHasher<T> hasher) {
        T first = list.peek();
        int pos = hasher.hash(first) & (buf.length-1);
        for(int i=0;i<buf.length;i++){
            RingBuffer<T> next = buf[pos];
            if(next==null){
                buf[pos] = list;
                return;
            }
            pos = (pos+1) & (buf.length-1);
        }
    }
}
