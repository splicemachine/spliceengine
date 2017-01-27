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

import org.spark_project.guava.base.Preconditions;
import org.spark_project.guava.cache.CacheStats;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;

import java.lang.ref.SoftReference;
import java.lang.reflect.Array;

/**
 * A Cache that is keyed by a long.
 *
 * This implementation uses linear probing plus "Robin-Hood Hashing" to
 * ensure that insert and lookup performance are both efficient, even with
 * a very high load factor (around 0.9). For details about Robin-hood hashing,
 * see Wikipedia(<a href="http://en.wikipedia.org/wiki/Hash_table#Robin_Hood_hashing" />)
 * for an overview, or
 * <a href="http://sebastiansylvan.com/2013/05/08/robin-hood-hashing-should-be-your-default-hash-table-implementation/">
 *     This blog entry</a> for a nice tutorial covering the essential elements. For details
 * on the technical nature of the strategy, <a href="http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.130.6339"/>
 * contains a good overview of worst-case performance.
 *
 * This implementation does not resize arrays--instead, it pre-computes an array
 * of size {@code 1.12*maxSize}, which means that when the cache is completely full,
 * it will have a load factor of ~ 0.9 (with Robin-Hood hashing, this is an effective size).
 *
 * This class is <em>not</em> thread-safe. External synchronization is necessary
 * to ensure that this cache retains correctness in the face of multi-threaded access.
 *
 * @author Scott Fines
 * Date: 9/22/14
 */
public class LongKeyedCache<T> {
    private final int maxSize;
    private final Holder[] elements;
    private final HolderFactory holderFactory;
    private final Hash32 hashFunction;

    /*A pre-stored value of the longest probe length in this*/
    private int longestProbeLength;

    /* mask for determining array positions. Cached for efficiency */
    private final int positionMask;

    Holder tail;
    Holder head;

    private int size;
    private Counter evictedCounter;
    private Counter requestCounter;
    private Counter hitCounter;
    private Counter missCounter;


    @SuppressWarnings("unchecked")
    private LongKeyedCache(int maxCacheSize,
                          boolean softReferences,
                          Hash32 hashFunction,
                          MetricFactory metricFactory) {
        this.hashFunction = hashFunction;

        int m = 1;
        while(m<maxCacheSize){
            m<<=1;
        }

        this.maxSize = m;
        float loadedCacheSize = 1.12f*m;
        while(m<loadedCacheSize){
            m<<=1;
        }
        this.elements = (Holder[])Array.newInstance(Holder.class,m);
        this.positionMask = m-1;

        if(softReferences){
            this.holderFactory = new HolderFactory() {
                @Override
                Holder newHolder() {
                    return new SoftHolder();
                }
            };
        }else{
            this.holderFactory = new HolderFactory() {
                @Override
                Holder newHolder() {
                    return new StrongHolder();
                }
            };
        }

        /*Metrics collection. If no metrics are desired, these will be no-op counters */
        this.evictedCounter = metricFactory.newCounter();
        this.requestCounter = metricFactory.newCounter();
        this.hitCounter = metricFactory.newCounter();
        this.missCounter = metricFactory.newCounter();
        this.head = tail = holderFactory.newHolder();
    }

    /**
     * @param key the key to lookup
     * @return the element attached to the specified key, or {@code null} if no element with
     * {@code key} is present in the cache
     */
    public T get(long key){
        requestCounter.increment();
        int position = hashFunction.hash(key) & positionMask;
        int probeLength = 0;
        do{
            Holder n = elements[position];
            if(n!=null && !n.isEmpty()){
                if(n.key==key){
                    hitCounter.increment();
                    return n.get();
                }
            }
            probeLength++;
            position = (position+1)& positionMask;

        }while(probeLength<=longestProbeLength);
        //we couldn't find it after probing it as long as the longest probe sequence.
        missCounter.increment();
        return null;
    }

    /**
     * Put the specified element in the cache.
     *
     * Note that if the specified key is already matched to an entry which is logically equals
     * (i.e. {@code value.equals(other) == true}), then the element will <em>not</em> be inserted again--only
     * one copy in a single location is kept (this avoids extraneous object creations).
     *
     * @param key the key for the element.
     * @param value the value for the element.
     */
    public boolean put(long key, T value){
        assert value!=null: "Cannot insert a null value!";
        evictIfFull();
        int hash = hashFunction.hash(key) & positionMask;
        int probeLength = 0;
        Holder toSet = null;
        int position = hash;
        boolean added=false;
        do{
            Holder n = elements[position];
            if(n==null||n.isEmpty()){
                if(toSet==null){
                    toSet = construct(key, value, hash,n);
                }
                elements[position] = toSet;
                /*
                 * We can return here without updating the longest probe length,
                 * because we would have broken from the length if we had exceeded
                 * that probe length
                 */
                added = true;
                break;
            }else if(n.holds(value)){
                //we already have this element in the array, so do nothing
                break;
            }else{
                int pD = probeDistance(n.hash,position);
                if(pD<probeLength){
                /*
                 * We have hit a Robin-Hood moment. Replace this position
                 * with the current one, then continue on with our new probe length to check
                 */
                    if(toSet==null){
                        toSet = construct(key, value, hash,null);
                    }
                    elements[position] = toSet;
                    toSet = n;
                    probeLength = pD;
                    hash = n.hash;
                    added = true;
                    position = (position+1) & positionMask;
                }else{
                    probeLength = incrementProbeLength(probeLength);
                    position = (position+1) & positionMask;
                }
            }
        }while(true);

        if(added)
            size++;

        return added;
    }

    private int incrementProbeLength(int currProbeLength) {
        int cPl = currProbeLength+1;
        if(cPl>longestProbeLength)
            longestProbeLength = cPl;
        return cPl;
    }

    private Holder construct(long key, T value, int hash,Holder existing) {
        Holder toSet = existing;
        if(toSet==null)
            toSet = holderFactory.newHolder();
        if (toSet.prev != null) {
            toSet.prev.next = toSet.next;
        }
        toSet.set(value);
        toSet.prev = tail;
        toSet.next = null;
        tail.next = toSet;
        tail = toSet;
        toSet.hash = hash;
        toSet.key = key;
        return toSet;
    }

    private void evictIfFull() {
        /*
         *Evict an entry if we have exceeded the maximum size of the cache
         */
        if(size>=maxSize){
            Holder h = head;
            while(h.isEmpty()){
                head = h.next; //unlink from LL
                h.next = null;
                h.prev = null;
                h = head;
            }
            h.clear(); //remove fields from the holder so that it can be re-used
            head = h.next;
            h.next = null;
            h.prev = null;
            //update stats
            evictedCounter.increment();
            size--;
        }
    }

    private int probeDistance(int hash,int position) {
        if(position>hash) return position-hash;
        else return hash-position;
    }

    /**
     * @return the current size of this cache
     */
    public int size(){ return size; }

    public CacheStats getStats(){
        return new CacheStats(hitCounter.getTotal(),missCounter.getTotal(),0l,0l,0l,evictedCounter.getTotal());
    }

    private abstract class HolderFactory{
        abstract Holder newHolder();
    }

    abstract class Holder {
        Holder next; //linked-list in order to clear elements
        Holder prev;
        int hash;
        long key;

        public void clear(){
            doClear();
            hash = -1;
        }

        public void set(T next){
            doSet(next);
        }

        public abstract boolean isEmpty();

        protected abstract void doClear();

        protected abstract void doSet(T next);

        public abstract T get();

        public abstract boolean holds(T value);
    }

    private class SoftHolder extends Holder{
        private SoftReference<T> reference;

        @Override protected void doSet(T next) { this.reference = new SoftReference<T>(next); }
        @Override protected void doClear() { this.reference = null;  }
        @Override public T get() { return reference.get(); }
        @Override public boolean holds(T value) { return value.equals(reference.get()); }

        @Override
        public boolean isEmpty() {
            if(reference==null) return true;
            if(reference.get()==null){
                clearReference();
                return true;
            }
            return false;
        }

        private void clearReference() {
				    /*
				     * The garbage collector collected the reference,
				     *  so we are empty whether we like it or not. Thus,
				     *  we ensure that the reference queue maintains integrity
				     *  by removing ourselves from it, and that our size count
				     *  remains accurate by decrementing the size element by one.
				     */
            clear();
            if(next!=null)
                next.prev = prev;
            if(prev!=null)
                prev.next = next;
            size--;
            evictedCounter.increment(); //maintain eviction stats for memory evictions
        }

    }

    private class StrongHolder extends Holder{
        private T ref;

        @Override public T get() { return ref; }
        @Override protected void doClear() { this.ref = null;  }
        @Override protected void doSet(T next) { this.ref = next; }
        @Override public boolean isEmpty() { return ref==null; }
        @Override public boolean holds(T value) { return value.equals(ref); }
    }

    public static <T> Builder<T> newBuilder(){
        return new Builder<T>();
    }
    public static class Builder<T>{
        private MetricFactory metricFactory = Metrics.noOpMetricFactory();
        private boolean softReferences = false;
        private Hash32 baseFunction = HashFunctions.utilHash();
        private int size = -1;

        public Builder<T> metricFactory(MetricFactory metricFactory) {
            this.metricFactory = metricFactory;
            return this;
        }

        public Builder<T> collectStats(){
            this.metricFactory = Metrics.basicMetricFactory();
            return this;
        }

        public Builder<T> withSoftReferences() {
            this.softReferences = true;
            return this;
        }

        public Builder<T> withHashFunction(Hash32 baseFunction) {
            this.baseFunction = baseFunction;
            return this;
        }

        public Builder<T> maxEntries(int size) {
            this.size = size;
            return this;
        }

        public LongKeyedCache<T> build(){
            Preconditions.checkArgument(size>0,"Cannot create a cache with a negative size!");
            return new LongKeyedCache<T>(size,softReferences,baseFunction,metricFactory);
        }
    }

}
