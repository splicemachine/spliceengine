package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class HashBuffer<K,T extends ExecRow> {
	protected int maxCapacity;
	protected T matchedRow = null;
    private final HashMap<K,T> buffer;
	
	public HashBuffer(int maxCapacity){
		this.maxCapacity = maxCapacity;
        buffer = Maps.newHashMapWithExpectedSize(maxCapacity);
    }

	@SuppressWarnings("LoopStatementThatDoesntLoop")
    public Entry<K,T> add(K key, T element){
		buffer.put(key, element);
        if(buffer.size()>maxCapacity){
           //evict the first entry returned by the buffer's iterator
            Set<Entry<K,T>> entries = buffer.entrySet();
            //intentionally doesn't loop, this is just simpler than creating an iterator and blah blah blah
            for(Entry<K,T> entry:entries){
                return entry;
            }
        }
		return null;
	}
	
	public boolean merge(K key, T element, HashMerger<K,T> merger){
		if ( (matchedRow = merger.shouldMerge(this, key)) == null) {
			return false;
		}
		merger.merge(this, matchedRow, element);
		return true;
	}
	
	public HashBuffer<K,T> finishAggregates(AggregateFinisher<K,T> aggregateFinisher) throws StandardException {
		HashBuffer<K,T> finalizedBuffer = new HashBuffer<K,T>(maxCapacity);
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

    public T get(K key) {
        return buffer.get(key);
    }

    public int size() {
        return buffer.size();
    }

    public Set<K> keySet(){
        return buffer.keySet();
    }

    public T remove(K key) {
        return buffer.remove(key);
    }

    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    public Collection<T> values() {
        return buffer.values();
    }

    public Set<Entry<K,T>> entrySet() {
        return buffer.entrySet();
    }

    public void clear() {
        buffer.clear();
    }
}
