package com.splicemachine.stats.frequency;

import com.splicemachine.hash.Hash32;
import com.splicemachine.utils.ComparableComparator;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

/**
 * @author Scott Fines
 *         Date: 12/8/14
 */
@NotThreadSafe
class ObjectSpaceSaver<T> implements FrequencyCounter<T> {
    private final Comparator<? super T> comparator;
    protected final Hash32 hashFunction;
    private final float loadFactor;
    private final int maxSize; //the maximum number of entries in the table

    private int[] hashCodes;
    private Entry[] entries;
    private int positionMask;
    private int expandPoint;

    protected int size; //the current size of the hashtable

    protected SizeBucket maxBucket;
    private SizeBucket minBucket = maxBucket = new SizeBucket(1);
    private long total;

//    private long totalCount = 0;

    public static <T extends Comparable<T>> ObjectSpaceSaver<T> create(Hash32 hashFunction,int maxSize){
        return new ObjectSpaceSaver<>(ComparableComparator.<T>newComparator(),hashFunction,maxSize);
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
        if(b>2*m)
            b=2*m;

        hashCodes = new int[b];
        //noinspection unchecked
        entries = (Entry[])Array.newInstance(Entry.class,b);
        positionMask = b-1;
        expandPoint = (int)(loadFactor*b); //allow some expansion
    }

    @Override
    public FrequentElements<T> frequentElements(int k) {
        Collection<FrequencyEstimate<T>> estimates = topKElements(k);
        return ObjectFrequentElements.topK(k,totalCount(),estimates,comparator);
    }

    @Override
    public FrequentElements<T> heavyHitters(float support) {
        Collection<FrequencyEstimate<T>> estimates = heavyItems(support);
        return ObjectFrequentElements.heavyHitters(support,totalCount(),estimates,comparator);
    }

    protected final Collection<FrequencyEstimate<T>> heavyItems(float support) {
        long threshold = (long)(total*support);
        Collection<FrequencyEstimate<T>> estimates = new ArrayList<>(size);
        SizeBucket b = maxBucket;
        while(b!=null){
            Entry e = b.firstEntry;
            Entry first = e;
            do{
                if(e.count()>=threshold)
                    estimates.add(e);
                e = e.next;
            }while(e!=null && e!=first);
            b = b.previous;
        }
        return estimates;
    }

    protected final Collection<FrequencyEstimate<T>> topKElements(int k) {
        k = Math.min(size,k);
        Collection<FrequencyEstimate<T>> estimates = new ArrayList<>(k);
        SizeBucket b = maxBucket;
        int added = 0;
        while(b!=null && added<k){
            Entry e = b.firstEntry;
            Entry first = e;
            do{
                boolean wasAdded = estimates.add(e);
                assert wasAdded: "Element "+ e+" already exists in set!";
                added++;
                e = e.next;
            }while(added<k && e!=null && e!=first);
            b = b.previous;
        }
        return estimates;
    }

    protected long totalCount() {
        return total;
    }

    /***********************************************************************/
    /*Modifiers*/

    @Override public void update(T item) { update(item,1l); }

    protected Entry holderEntry = newEntry();
    @Override
    public void update(T item, long count) {
        holderEntry.set(item);
        doUpdate(count);
    }

    protected final void doUpdate(long count) {
        int hashCode = holderEntry.hashCode();
        Entry entry = getEntry(holderEntry,hashCode);
        boolean evicted = false;
        if(entry==null){
            //no entry exists in the hashtable
            if(size ==maxSize){
                entry = evict();
                evicted=true;
            }else
                entry = newEntry();
            setValue(holderEntry,entry);
            //stash a reference to the hashcode to avoid the potentially expensive recomputation cost
            entry.hashCode = hashCode;

            sizedInsert(entry, hashCode);
            if(!evicted)
                size++;
        }
        //increment the count
        entry.increment(count);
        total+=count;
    }


    /**********************************************************************/
    /*Overrideable methods*/

    protected void setValue(Entry holderEntry, Entry entry) {
        entry.value = holderEntry.value;
    }

    protected Entry newEntry() {
        return new Entry();
    }

    protected class Entry implements FrequencyEstimate<T>{
        private SizeBucket bucket;
        private long epsilon;
        private T value;

        /*circular linked list of entries*/
        private Entry previous;
        protected Entry next;

        public transient int hashCode = 0;

        private void increment(long count) {
            long newV = this.bucket==null?count:bucket.count+count;
            SizeBucket b = getIncrementedBucket(this.bucket,newV,this);
            if(b!=bucket){
                if(bucket!=null){
                    bucket.remove(this);
                    bucket.size--;
                    if(bucket.size==0)
                        bucket.removeBucket();
                }
                b.add(this);
                bucket = b;
            }
        }

        @Override
        public FrequencyEstimate<T> merge(FrequencyEstimate<T> otherEst) {
            increment(otherEst.count());
            this.epsilon = Math.max(this.epsilon,otherEst.error());
            return this;
        }

        @Override public T getValue() { return value; }
        @Override public long count() { return bucket.count; }
        @Override public long error() { return epsilon; }

        @Override
        public String toString() {
            return "("+getValue()+","+count()+","+error()+")";
        }

        public void set(T item) {
            this.value = item;
            this.hashCode =0;
        }

        @Override
        public int hashCode() {
            if(hashCode==0) {
                hashCode = computeHash();
            }
            return hashCode;
        }

        protected int computeHash() {
            if(value==null) return 0;

            int hash = hashFunction.hash(value.hashCode());
            if (hash == 0)
                hash = 1;
            return hash;
        }

        public boolean equals(Entry o) { return this == o || value.equals(o.value); }
    }

    /*****************************************************************************************************************/
    /*private helper methods*/

    private SizeBucket getIncrementedBucket(SizeBucket oldValue,long newValue,Entry entry) {
        if(oldValue==null)
            oldValue = minBucket;

        SizeBucket floor = oldValue;
        SizeBucket n = floor.next;
        while(n!=null && n.count<=newValue){
            floor = n;
            n = n.next;
        }

        SizeBucket addedBucket;
        if(floor.count==newValue)
            return floor;
        else if(floor.size==0 || (floor.size==1 && floor.firstEntry==entry)){
            floor.count = newValue;
            //unlink floor, then find a new floor
            addedBucket = floor;
            n = floor.next;
            while(n!=null && n.count <=floor.count){
                floor = n;
                n = n.next;
            }
            if(floor!=addedBucket){
                addedBucket.next=null;
                addedBucket.previous=null; //we need to re-link it now
            }
        } else{
           addedBucket = new SizeBucket(newValue);
        }

        //link the new bucket
        if(floor.count< addedBucket.count){
            addedBucket.next = floor.next;
            addedBucket.previous = floor;
            if(floor.next!=null)
                floor.next.previous=addedBucket;
            floor.next=addedBucket;
        }else if(floor.count>addedBucket.count){
            /*
             * This can happen if we are adding an entry which is below
             * the current minBucket
             */
            addedBucket.next = floor;
            addedBucket.previous = floor.previous;
            if(floor.previous!=null)
                floor.previous.next = floor;
            floor.previous = addedBucket;
        }

        if(minBucket.count>addedBucket.count)
            minBucket = addedBucket;
        if(maxBucket.count<addedBucket.count)
            maxBucket = addedBucket;
        return addedBucket;
    }


    /*********************************************************************/
    /*hashtable manipulation methods*/
    private void sizedInsert(Entry entry, int hashCode){
        if(size==expandPoint)
            resize();
        insert(entry,hashCode);
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
                place(pos,toInsert,code);
                toInsert = e;
                code = hC;
                chainLength = probeLength;
            }
            pos = (pos+1) & positionMask;
            chainLength++;
        }
    }

    private void place(int pos,Entry toInsert,int code){
        entries[pos] = toInsert;
        hashCodes[pos] = code;
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
                hashCodes[p] = 0;
                return;
            }else{
                int probeLength = getProbeDistance(hC,n);
                if(probeLength==0){
                    hashCodes[p] = 0;
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


    private Entry getEntry(Entry item,int hashCode) {
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
                if(item.equals(e)){
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

    protected final class SizeBucket{
        private long count;
        protected Entry firstEntry;

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
            entry.previous = null;
            entry.next=null;
            if(entry==firstEntry)
                firstEntry = n;
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

