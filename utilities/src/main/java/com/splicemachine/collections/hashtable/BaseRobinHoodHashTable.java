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

package com.splicemachine.collections.hashtable;

import java.util.*;

/**
 * Implements a Hashtable using RobinHood hashing
 * @author Scott Fines
 *         Date: 10/7/14
 */
public abstract class BaseRobinHoodHashTable<K,V> implements HashTable<K,V> {
    private int[] hashCodes;
    private Object[] keys;
    private Object[] values;
    private int size;

    private final float loadFactor;

    private int longestProbeLength;
    private int positionMask;
    private int capacity;

    protected BaseRobinHoodHashTable(int initialSize, float loadFactor) {
        this.loadFactor = loadFactor;

        int s = 1;
        while(s<initialSize)
            s<<=1;
        this.hashCodes = new int[s];
        this.keys = new Object[s];
        this.values = new Object[s];
        this.positionMask = s-1;
        this.capacity = (int)(loadFactor*hashCodes.length);
    }

    @Override
    public float load() {
        return ((float)size)/hashCodes.length;
    }

    @Override public int size() { return size; }
    @Override public boolean isEmpty() { return size<=0; }

    @Override
    @SuppressWarnings("unchecked")
    public boolean containsKey(Object key) {
        int hashCode = hash((K)key);
        int pos = hashCode & positionMask;
        for(int i=0;i<=longestProbeLength;i++){
            int n = hashCodes[pos];
            if(n==hashCode){
                Object k = keys[n];
                if(k.equals(key)) return true;
            }
            pos = (pos+1) & positionMask;
        }
        return false;
    }


    @Override
    public boolean containsValue(Object value) {
        for(int i=0;i<hashCodes.length;i++){
            if(hashCodes[i]!=0){
                Object v = values[i];
                if(v.equals(value)) return true;
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        int hashCode = hash((K)key);
        int pos = hashCode & positionMask;
        for(int i=0;i<=longestProbeLength;i++){
            int n = hashCodes[pos];
            if(n==hashCode){
                Object k = keys[pos];
                if(key.equals(k)){
                    return (V)values[pos];
                }
            }else if(n==0)
                return null; //couldn't find it

            pos = (pos+1) & positionMask;
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        if(size>=capacity){
            //we have exceeded the size limit, so rehash the structure
            expandCapacity();
        }
        V valueToReturn = doPut(key, value);
        if(valueToReturn==null) size++;
        return valueToReturn;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V remove(Object key) {
        return remove((K)key,false);
    }

    @Override
    public V remove(K key, boolean forceRemoveValue) {
        /*
         * This removal strategy uses a "backwards deletion" strategy to ensure
         * correctness. Essentially, it finds the element to delete, and then removes it.
         * Then, after removing, it iterates through the following items until either
         *
         * A) it find an empty location
         * B) it finds a location with a probe length of 0
         *
         * This keeps the hashtable compact, which keeps insertion and deletion performance constant.
         */
        int hashCode = hash(key);
        int pos = hashCode & positionMask;
        for(int i=0;i<=longestProbeLength;i++){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                //we found a possible match, check it
                Object k = keys[pos];
                if(k==null){
                    /*
                     * We found an empty slot, and we just so happen to have a 0 for a hash code.
                     * That sucks, we didn't find the element
                     */
                    return null;
                } else if(key.equals(k)){
                    /*
                     * We found the entry, and we want to remove it.
                     * If the hashCode is 0, then we have to ensure that
                     * we remove the key and value, because otherwise it might
                     * return the value after it's been deleted
                     */
                    return remove(pos,hashCode==0);
                }
            }else if(hC==0){
                //empty slot, we didn't find it
                break;
            }
            pos = (pos+1) & positionMask;
        }

        //we didn't find any matching elements, so nothing to return
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for(Map.Entry<? extends K,? extends V> entry:m.entrySet()){
            put(entry.getKey(),entry.getValue());
        }
    }

    @Override public void clear() { clear(false); }

    public void clear(boolean removeValues){
        size=0;
        Arrays.fill(hashCodes,0); //fill the hashCode with empty
        if(removeValues){
            Arrays.fill(keys,null);
            Arrays.fill(values,null);
        }
    }

    @Override public Set<K> keySet() { return new KeySet(); }
    @Override public Collection<V> values() { return new Values(); }
    @Override public Set<Entry<K, V>> entrySet() { return new EntrySet(); }


    protected abstract int hashCode(K key);

    protected abstract V merge(V newValue, V existing);

   /******************************************************************************************************************/
    /*private helper methods*/

   private int hash(K key){
       int hashCode = hashCode(key);
        /*
         * 0 is a reserved hash code in our system, because it is used to indicate that there
         * is no entry at that position. As a result, our hash codes can't be zero. We avoid this
         * issue by moving them to the 1 position. This may cause more conflicts than otherwise would
         * happen, but it maintains correctness
         */
       if(hashCode==0)
           hashCode=1;
       return hashCode;
   }

    @SuppressWarnings("unchecked")
    private void expandCapacity() {
        int currSize = hashCodes.length;
        int newSize = Math.min(Integer.MAX_VALUE,2*currSize);

        //rehash the entries
        int[] oldHashCodes = hashCodes;
        Object[] oldKeys = keys;
        Object[] oldValues = values;

        hashCodes = new int[newSize];
        keys = new Object[newSize];
        values = new Object[newSize];
        capacity = (int)(loadFactor*newSize);
        positionMask = newSize-1;
        for(int i=0;i<oldHashCodes.length;i++){
            int hC = oldHashCodes[i];
            if(hC==0) continue; //we know that that element is not present

            K key = (K)oldKeys[i];
            V value = (V)oldValues[i];
            doPut(key, value);
        }
    }

    @SuppressWarnings("unchecked")
    private V doPut(K key, V value) {
        int hashCode = hash(key);
        int pos = hashCode & positionMask;
        int probeLength = 0;
        K keyToPlace = key;
        V valueToPlace = value;
        V valueToReturn = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==0){
                /*
                 * We have an empty slot, so place it directly
                 */
                keys[pos] = keyToPlace;
                values[pos] = valueToPlace;
                hashCodes[pos] = hashCode;
                break;
            }else if(hC==hashCode){
                /*
                 * it's possible that someone else with that key already exists, in which case we replace
                 */
                Object k = keys[pos];
                if(k==null){
                /*
                 * We have an empty slot, so place it directly
                 */
                    keys[pos] = keyToPlace;
                    values[pos] = valueToPlace;
                    hashCodes[pos] = hashCode;
                    break;
                } else if(keyToPlace.equals(k)){
                    valueToReturn = (V)values[pos];

                    values[pos] = merge(valueToPlace,(V)values[pos]);
                    break;
                }
            }
            int probeLengthCode = getProbeDistance(pos,hC);
            if(probeLengthCode<probeLength){
                   /*
                    * We've reached an element with a shorter probe length than us. In this case,
                    * we swap the element, and adjust the probe length accordingly
                    */
                K k = (K)keys[pos];
                keys[pos] = keyToPlace;
                V v = (V)values[pos];
                values[pos] = valueToPlace;
                hashCodes[pos] = hashCode;

                probeLength = probeLengthCode;
                hashCode = hC;
                keyToPlace = k;
                valueToPlace = v;
            }
            probeLength++;
            pos = (pos+1) & positionMask;
        }
        if(longestProbeLength<probeLength)
            longestProbeLength = probeLength;
        return valueToReturn;
    }

    @SuppressWarnings("unchecked")
    private V remove(int pos,boolean clearValues) {
        V toReturn = (V) values[pos];
        hashCodes[pos] = 0; //tell the world this slot is empty
        if(clearValues){
            keys[pos] = null;
            values[pos] = null;
        }
        backwardsDeletion(pos,clearValues);
        return toReturn;
    }

    private void backwardsDeletion(int pos,boolean clearValues) {
        int adjustPos = (pos+1) & positionMask;
        for(int j=0;j<=longestProbeLength;j++){
            int hC = hashCodes[adjustPos];
            if(hC==0) break;
            int probeDistance = getProbeDistance(adjustPos,hC);
            if(probeDistance<=0) break; //no need to adjust further

            //we need to move this to one position back
            keys[adjustPos-1] = keys[adjustPos];
            values[adjustPos-1] = values[adjustPos];
            hashCodes[adjustPos] = 0;
            if(clearValues){
                //help the garbage collector by dereferencing the actual elements
                keys[adjustPos] = null;
                values[adjustPos] = null;
            }
        }
    }

    private int getProbeDistance(int currentPosition, int hash) {
        /*
         * Get the distance from the current position to where the hashCode
         * says it *should* be located.
         */
        int hc = (hash & positionMask);
        int dist = currentPosition-hc;
        if(currentPosition<hc)
            dist+=positionMask+1;
        return dist;
    }

    /*internal data structure classes*/
    private final class Values extends AbstractCollection<V>{
        @Override public Iterator<V> iterator() { return new ValueIterator(); }
        @Override public int size() { return size; }
        @Override public boolean contains(Object o) { return containsValue(o); }
        @Override public void clear() { BaseRobinHoodHashTable.this.clear(); }
    }

    private final class EntrySet extends AbstractSet<Map.Entry<K,V>>{
        @Override public Iterator<Entry<K,V>> iterator() { return new EntryIterator(); }
        @Override public int size() { return size; }
        @Override
        public boolean remove(Object o) {
            return o instanceof Entry
                    && BaseRobinHoodHashTable.this.remove(((Entry) o).getKey()) != null;
        }
        @Override public void clear() { BaseRobinHoodHashTable.this.clear(); }
    }

    private final class KeySet extends AbstractSet<K>{

        @Override public Iterator<K> iterator() { return new KeyIterator(); }
        @Override public int size() { return size; }
        @Override public boolean contains(Object o) { return containsKey(o); }
        @Override
        public boolean remove(Object o) {
            return BaseRobinHoodHashTable.this.remove(o)!=null;
        }
        @Override public void clear() { BaseRobinHoodHashTable.this.clear(); }
    }

    private abstract class HashIterator<T> implements Iterator<T>{
        protected int position = 0;
        protected boolean eaten = true;
        private int expectedSize = size;

        @Override
        public boolean hasNext() {
            if(!eaten) return true;
            if(expectedSize!=size)
                throw new ConcurrentModificationException();
            if(position>=hashCodes.length) return false; //off the end of the map
            while(position<hashCodes.length && hashCodes[position]==0)
                position++;
            eaten = false;
            return position<hashCodes.length;
        }
        @Override
        public void remove() {
            BaseRobinHoodHashTable.this.remove(position, false);
        }
    }

    private class ValueIterator extends HashIterator<V> {

        @SuppressWarnings("unchecked")
        @Override
        public V next() {
            if(!hasNext()) throw new NoSuchElementException();
            eaten = true;
            return (V)values[position];
        }
    }

    private class KeyIterator extends HashIterator<K> {

        @SuppressWarnings("unchecked")
        @Override
        public K next() {
            if(!hasNext()) throw new NoSuchElementException();
            eaten = true;
            return (K)keys[position];
        }
    }

    private class EntryIterator extends HashIterator<Map.Entry<K,V>>{
        private final MutableEntry<K,V> entry = new MutableEntry<K,V>();
        @Override
        public Entry<K, V> next() {
            entry.position = position;
            eaten =true;
            return entry;
        }
    }

    @SuppressWarnings("unchecked")
    private class MutableEntry<K, V> implements Entry<K,V>{
        int position;
        @Override public K getKey() { return (K)keys[position]; }
        @Override public V getValue() { return (V)values[position]; }

        @Override
        public V setValue(V value) {
            V old = (V)values[position];
            values[position] = value;
            return old;
        }
    }
}
