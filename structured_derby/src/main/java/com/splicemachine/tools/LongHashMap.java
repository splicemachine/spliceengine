package com.splicemachine.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Integer-keyed HashMap.
 *
 * This mechanism uses Open Addressing, combined with Linear Probing to resolve hash collisions. If the underlying
 * table is full, then it will be expanded by a configurable <em>growth factor</em>, unless the {@code evict} property
 * is set. If {@code evict} is set, then the <em>last seen</em> record in the map will be emitted.
 *
 * @author Scott Fines
 * Created on: 9/16/13
 */
@SuppressWarnings("unchecked")
public abstract class LongHashMap<T> {

    protected LongEntry<T>[] elements;

    private LongHashMap(int size) {
        this.elements = (LongEntry<T>[])new LongEntry[size];
    }

    public T get(long key){
        int hash = hash(key);
        int length = elements.length;
        int i = indexFor(hash, length);
        int visited=0;
        do{
            if(elements[i] ==null||elements[i].empty) return null; //doesn't exist
            else if(elements[i].key==key)
                return elements[i].entry;
            else{
                visited++;
                i = (i+1)&(length-1);
            }
        }while(visited<length);
        return null;
    }

    public LongEntry<T> put(long key, T value){
        int hash= hash(key);
        int length = elements.length;
        int i = indexFor(hash,length);
        int visited=0;
        LongEntry<T> entry;
        do{
            entry = elements[i];
            if(entry==null){
                //we have encountered an empty slot, insert!
                entry = new LongEntry<T>();
                elements[i] = entry;
            }else if(entry.empty){
                //also empty, but we're re-using the object
                entry = elements[i];
            }else if(entry.key==key){
                //same key
                entry.entry = value;
                return null;
            }else{
                //not empty, and different keys
                visited++;
                i = (i+1)&(length-1);
                continue;
            }
            //we have an entry, so set the positions
            entry.key = key;
            entry.entry = value;
            entry.empty=false;
            return null;
        }while(visited<length);

        //we've gone through the entire element set--we don't have enough room. Time to expand and/or evict
        return outOfSpace(entry,key,value);
    }

    public List<LongEntry<T>> clear(){
        List<LongEntry<T>> entries = Lists.newArrayList();
        int i=0;
        for(LongEntry<T> entry:elements){
            if(entry!=null&&!entry.empty){
                entries.add(entry);
            }
            elements[i] = null;
            i++;
        }
        return entries;
    }

    protected abstract LongEntry<T> outOfSpace(LongEntry<T> entry, long key, T value);

    private int indexFor(int hash, int length) {
        return hash & (length-1);
    }

    protected int hash(long key) {
        int hashCode = (int)(key^(key>>>32));

        hashCode ^= (hashCode>>>20)^(hashCode>>>12);
        return hashCode^(hashCode>>>7)^(hashCode>>>4);
    }

    public static <T> LongHashMap<T> evictingMap(int maxEntries){
        //find smallest power of 2 greater than maxEntries
        int s = 1;
        while(s<maxEntries){
            s <<=1;
        }
        return new LongHashMap<T>(s) {
            @Override
            protected LongEntry<T> outOfSpace(LongEntry<T> entry, long key, T value) {
                LongEntry<T> toEvict = new LongEntry<T>(entry);
                entry.key = key;
                entry.entry = value;
                entry.empty = false;
                return toEvict;
            }
        };
    }

    public static <T> LongHashMap<T> growingMap(int initialSize,final float growthFactor){
        Preconditions.checkArgument(growthFactor>1.0f,"Growth Factor must be larger than 1!");
        return new LongHashMap<T>(initialSize) {
            @Override
            protected LongEntry<T> outOfSpace(LongEntry<T> entry, long key, T value) {
                LongEntry<T> [] old = elements;
                int newLength = (int)(Math.ceil(growthFactor*elements.length));

                elements = (LongEntry<T>[])(new LongEntry[newLength]);
                for(LongEntry<T> element:old){
                    int hash = hash(element.key);
                    int i = (hash & (newLength-1));
                    int visited=0;
                    while(visited<newLength){
                        if(elements[i]==null){
                            elements[i] = element;
                        }
                    }
                }
                LongEntry<T> next = new LongEntry<T>();
                next.empty=false;
                next.key = key;
                next.entry = value;
                elements[old.length] = next;
                return null;
            }
        };
    }

    public static class LongEntry<T> {
        private long key;
        private T entry;
        private boolean empty = true;

        public LongEntry() {
        }

        public LongEntry(LongEntry<T> entry) {
            if(entry!=null){
                this.key = entry.key;
                this.entry = entry.entry;
                this.empty = entry.empty;
            }
        }

        public T getValue() {
            return entry;
        }

        public long getKey() {
            return key;
        }
    }
}
