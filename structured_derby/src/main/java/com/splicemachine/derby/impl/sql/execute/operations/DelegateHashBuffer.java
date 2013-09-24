package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class DelegateHashBuffer<K,T extends ExecRow> implements HashBuffer<K,T> {
	protected int maxCapacity;
	protected T matchedRow = null;
    private final HashMap<K,T> buffer;
	
	public DelegateHashBuffer(int maxCapacity){
		this.maxCapacity = maxCapacity;
        buffer = Maps.newHashMapWithExpectedSize(maxCapacity);
    }

	@Override
    public Entry<K,T> add(K key, T element){
		buffer.put(key, element);
        if(buffer.size()>maxCapacity){
           //evict the first entry returned by the buffer's iterator
            Iterator<Entry<K,T>> entryIterator = buffer.entrySet().iterator();
            while(entryIterator.hasNext()){
                Entry<K,T> toEvict = entryIterator.next();
                if (toEvict.getKey().equals(key)) // Do Not Allow Self Eviction
                	continue;
                entryIterator.remove();;
                return toEvict;
            }
        }
		return null;
	}
	
	@Override
    public boolean merge(K key, T element, HashMerger<K, T> merger){
		if ( (matchedRow = merger.shouldMerge(this, key)) == null) {
			return false;
		}
		merger.merge(this, matchedRow, element);
		return true;
	}
	
	@Override
    public HashBuffer<K,T> finishAggregates(AggregateFinisher<K, T> aggregateFinisher) throws StandardException {
		DelegateHashBuffer<K,T> finalizedBuffer = new DelegateHashBuffer<K,T>(maxCapacity);
		for (Entry<K,T> entry: buffer.entrySet()) {
			finalizedBuffer.buffer.put(entry.getKey(), aggregateFinisher.finishAggregation(entry.getValue()));
		}
		buffer.clear();
		return finalizedBuffer;
	}
	
	@Override
	public String toString(){
		return "RingBuffer{maxSize="+maxCapacity+",LinkedHashMap="+super.toString()+"}";
	}

    @Override
    public T get(K key) {
        return buffer.get(key);
    }

    @Override
    public int size() {
        return buffer.size();
    }

    @Override
    public Set<K> keySet(){
        return buffer.keySet();
    }

    @Override
    public T remove(K key) {
        return buffer.remove(key);
    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    @Override
    public Collection<T> values() {
        return buffer.values();
    }

    @Override
    public Set<Entry<K,T>> entrySet() {
        return buffer.entrySet();
    }

    @Override
    public void clear() {
        buffer.clear();
    }
}
