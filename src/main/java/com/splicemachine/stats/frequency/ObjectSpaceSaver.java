package com.splicemachine.stats.frequency;

import com.splicemachine.hash.Hash32;
import com.splicemachine.utils.ComparableComparator;

import java.lang.reflect.Array;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/8/14
 */
public class ObjectSpaceSaver<T> implements FrequencyCounter<T> {
    private final Comparator<? super T> comparator;
    private final Hash32 hashFunction;
    private final float loadFactor;
    private final int maxSize; //the maximum number of entries in the table

    private int[] hashCodes;
    private Entry[] entries;
    private int positionMask;
    private int expandPoint;

    private int size; //the current size of the hashtable

    private SizeBucket maxBucket;
    private SizeBucket minBucket = maxBucket = new SizeBucket(1);

    public static <T extends Comparable<T>> ObjectSpaceSaver<T> create(Hash32 hashFunction,int maxSize){
        return new ObjectSpaceSaver<T>(ComparableComparator.<T>newComparator(),hashFunction,maxSize);
    }
    public ObjectSpaceSaver(Comparator<? super T> comparator,Hash32 hashFunction, int maxSize) {
        this(comparator,hashFunction, maxSize,16,0.85f);
    }

    public ObjectSpaceSaver(Comparator<? super T> comparator,Hash32 hashFunction, int maxSize, int initialSize,float loadFactor) {
        this.comparator = comparator;
        this.hashFunction = hashFunction;
        this.loadFactor = loadFactor;
        int m = 1;
        while(m<maxSize)
            m<<=1;
        this.maxSize = m;

        int b = 1;
        while(b<initialSize)
            b<<=1;
        if(b>m)
            b=m;

        hashCodes = new int[b];
        //noinspection unchecked
        entries = (Entry[])Array.newInstance(Entry.class,b);
        positionMask = b-1;
        expandPoint = (int)(loadFactor*b); //allow some expansion
    }

    /*****************************************************************************************************************/
    /*Accessors*/
    @Override
    public Set<? extends FrequencyEstimate<T>> getFrequentElements(float support) {
        return null;
    }

    @Override
    public Set<? extends FrequencyEstimate<T>> getMostFrequentElements(int k) {
        return null;
    }

    @Override
    public FrequentElements<T> frequentElements(int k) {
        k = Math.min(size,k);
        Collection<FrequencyEstimate<T>> estimates = new ArrayList<FrequencyEstimate<T>>(k);
        SizeBucket b = maxBucket;
        int added = 0;
        while(b!=null && added<k){
            Entry e = b.firstEntry;
            Entry first = e;
            do{
                estimates.add(e);
                added++;
                e = e.next;
            }while(added<k && e!=null && e!=first);
            b = b.previous;
        }
        return new ObjectFrequentElements<T>(estimates,comparator);
    }

    @Override
    public Iterator<FrequencyEstimate<T>> iterator() {
        return null;
    }

    /***********************************************************************/
    /*Modifiers*/

    @Override public void update(T item) { update(item,1l); }

    @Override
    public void update(T item, long count) {
        int hashCode = hash(item);
        Entry entry = getEntry(item,hashCode);
        if(entry==null){
            //no entry exists in the hashtable
            if(size ==maxSize){
                entry = evict();
            }else
                entry = newEntry();
            entry.value = item;
            //stash a reference to the hashcode to avoid the potentially expensive recomputation cost
            entry.hashCode = hashCode;

            sizedInsert(entry, hashCode);
        }
        //increment the count
        entry.increment(count);
    }

    /**********************************************************************/
    /*Overrideable methods*/
    protected int hashCode(T item) {
        return item.hashCode();
    }

    protected boolean equals(Entry entry, T item) {
        return item.equals(entry.value);
    }

    protected class Entry implements FrequencyEstimate<T>{
        private SizeBucket bucket;
        private long epsilon;
        private T value;

        /*circular linked list of entries*/
        private Entry previous;
        private Entry next;

        public transient int hashCode;

        private void increment(long count) {
            long newV = this.bucket==null?count:bucket.count+count;
            SizeBucket b = getIncrementedBucket(this.bucket,newV);
            if(b!=bucket){
                if(bucket!=null){
                   bucket.remove(this);
                }
                b.add(this);
                bucket = b;
            }
        }

        @Override public T getValue() { return value; }
        @Override public long count() { return bucket.count; }
        @Override public long error() { return epsilon; }

        @Override
        public String toString() {
            return "("+value+","+count()+","+error()+")";
        }
    }

    private SizeBucket getIncrementedBucket(SizeBucket oldValue, long newValue) {
        SizeBucket nextBucket;
        if(oldValue==null){
            nextBucket = minBucket;
        }else{
            nextBucket = oldValue;
        }
        SizeBucket n = nextBucket;
        while(n!=null && n.count<=newValue){
            nextBucket = n;
            n = n.next;
        }
        if(nextBucket.count==newValue){
            return nextBucket; //we have found the proper bucket, nothing more to do
        }else {
                /*
                 * In this case, nextBucket is the largest size < newValue,
                 * so we have a special case: If nextBucket==oldValue and it only contains one element,
                 * then we can just set the count to the new value, and avoid an object
                 * creation; otherwise, we need to create a new bucket to hold our value
                 */
            if(nextBucket==oldValue && nextBucket.size==1){
                nextBucket.count = newValue;
            }else{
                /*
                 * We either have a different bucket, or we have more than
                 * one entry in the bucket, so we'll need to create a new bucket and link it
                 */
                SizeBucket b = new SizeBucket(newValue);
                if(nextBucket.count<newValue){
                    b.previous = nextBucket;
                    b.next = nextBucket.next;
                    if(nextBucket.next!=null)
                        nextBucket.next.previous = b;
                    nextBucket.next = b;
                    nextBucket = b;
                }else{
                    /*
                     * We have a minbucket situation here, where we are < than minBucket
                     */
                    b.next = nextBucket;
                    b.previous = nextBucket.previous;
                    nextBucket.previous = b;
                    nextBucket = b;
                }
            }

            if(nextBucket.count>maxBucket.count)
                maxBucket = nextBucket;
            if(minBucket.count>nextBucket.count)
                minBucket = nextBucket;
            return nextBucket;
        }
    }


    /*****************************************************************************************************************/
    /*private helper methods*/
    private Entry newEntry() {
        return new Entry();
    }

    private int hash(T item){
        int hash = hashFunction.hash(hashCode(item));
        /*
         * We can't allow a hashCode==0, because we use 0 to indicate that
         * a slot is empty. Thus, we adjust all 0 hashCodes to 1. This will
         * cause slightly more collisions than normal, but a well-formed uniform hash function
         * is pretty unlikely to generate 0s anyway.
         */
        if(hash==0)
            hash=1;
        return hash;

    }

    /*********************************************************************/
    /*hashtable manipulation methods*/
    private void sizedInsert(Entry entry, int hashCode){
        if(size==expandPoint)
            resize();
        insert(entry,hashCode);
        size++;
    }

    private void insert(Entry entry, int hashCode) {
        int pos = hashCode & positionMask;
        Entry toInsert = entry;
        int code = hashCode;
        int chainLength = 0;
        while(true){
            int hC = hashCodes[pos];
            if(hC==0){
                //we've reached an empty slot, so insert and break
                entries[pos] = toInsert;
                hashCodes[pos] = code;
                break;
            }
            int probeLength = getProbeDistance(hC,pos);
            if(probeLength<chainLength){
                /*
                 * We have found an element with a shorter probe length than ours,
                 * swap it's position with ours to keep the chain length shorter
                 */
                Entry e = entries[pos];
                entries[pos] = toInsert;
                hashCodes[pos] = code;
                toInsert = e;
                code = hC;
                chainLength = probeLength;
            }
            pos = (pos+1) & positionMask;
            chainLength++;
        }
    }

    private int getProbeDistance(int hash, int currentPosition) {
 /*
         * Get the distance from the current position to where the hashCode
         * says it *should* be located.
         */
        int hc = (hash & positionMask);
        int dist = currentPosition-hc;
        if(currentPosition<hc)
            dist+= positionMask+1;
        return dist;
    }

    private void resize() {
        /*
         * Ensure that we have enough space to maintain the specified load factor.
         */

        int newSize = 2*hashCodes.length;
        int newExpandPoint = (int)(loadFactor*newSize);
        if(newExpandPoint>maxSize)
            newExpandPoint = newSize; //stop expanding once we reach the maximum size
        expandPoint = newExpandPoint;

        int[] oldHashCodes = hashCodes;
        Entry[] oldEntries = entries;
        hashCodes = new int[newSize];
        //noinspection unchecked
        entries = (Entry[]) Array.newInstance(Entry.class,newSize);
        positionMask = newSize-1;

        for(int i=0;i<oldHashCodes.length;i++){
            int hC = oldHashCodes[i];
            if(hC==0) continue;
            Entry e = oldEntries[i];
            insert(e,hC);
        }
    }

    private void remove(Entry entry,int hashCode){
        int pos = hashCode & positionMask;
        int initialPos = pos;
        do{
            int hC = hashCodes[pos];
            if(hC==0){
                //we've hit an empty slot--I guess entry isn't there after all
                return;
            }else if(hC==hashCode){
                //potential match--check equality to be sure
                Entry e = entries[pos];
                if(e==entry){
                    backwardsDelete(pos);
                    return;
                }
            }
            pos = (pos+1) & positionMask;
        }while(initialPos!=pos);
    }

    private void backwardsDelete(int pos) {
        int p = pos;
        do{
            int n = (p+1) & positionMask;
            int hC = hashCodes[n];
            if(hC==0){
                //we are done!
                return;
            }else{
                int probeLength = getProbeDistance(hC,n);
                if(probeLength==0){
                    //We have hit a position that is in the correct location already, so we are done
                    return;
                }
                hashCodes[p] = hC;
                entries[p] = entries[n];
            }
            p = (p+1) & positionMask;
        }while(p!=pos);
    }

    private Entry evict() {
        long count = minBucket.count;
        Entry toRemove = minBucket.removeFirst();
        toRemove.epsilon = count;
        remove(toRemove,toRemove.hashCode);
        return toRemove;
    }


    private Entry getEntry(T item,int hashCode) {
        int pos = hashCode & positionMask;
        int initialPos = pos;
        do{
            int hC = hashCodes[pos];
            if(hC==0){
                //empty slot, so return null (we have to insert)
                return null;
            }else if(hC==hashCode){
                /*
                 * we have an element which matches out hashcode.
                 * Unfortunately, we may have hashcode collisions, so we will
                 * have to do an explicit equality check here just to be sure
                 */
                Entry e = entries[pos];
                if(equals(e,item)){
                    return e;
                }
            }
            pos = (pos+1) & positionMask;
        } while(pos!=initialPos); //loop until we've gone through the entire space
        /*
         * We reached the end of the longest chain without finding a match or an empty slot,
         * so we have to insert a new entry.
         */
        return null;
    }

    private class SizeBucket{
        private long count;
        private Entry firstEntry;

        private SizeBucket next;
        private SizeBucket previous;
        public int size = 0;

        public SizeBucket(long count) {
            this.count = count;
        }

        public Entry removeFirst() {
            Entry e = firstEntry;
            remove(e);
            return e;
        }

        public void add(Entry e){
            if(firstEntry==null)
                firstEntry = e;
            else{
                /*
                 * our list of Entries is a circular linked list, so firstEntry's previous is the
                 * last element in the list
                 */
                e.next = firstEntry;
                e.previous = firstEntry.previous;
                if(e.previous==null){
                    //firstEntry is the only element in the list, so circularly link it
                    e.previous = firstEntry;
                    firstEntry.next = e;
                }
                e.previous.next = e;
                firstEntry.previous = e; //ensure the circularity

            }
            size++;

        }

        public void remove(Entry entry) {
            //unlink the entry
            Entry p = entry.previous;
            Entry n = entry.next;
            if(n!=null)
                n.previous = p;
            if(p!=null)
                p.next = n;
            size--;
            if(size==0){
                removeBucket();
            }else{
                if(entry==firstEntry)
                    firstEntry = n;
                if(size==1) {
                    entry.next = null;
                    entry.previous = null;
                }

            }

        }

        private void removeBucket() {
            //this bucket is empty, so remove it
            if(next!=null)
                next.previous = previous;
            if(previous!=null)
                previous.next = next;
            if(minBucket==this)
                minBucket = next;
            if(maxBucket==this)
                maxBucket = previous;
        }
    }
}

