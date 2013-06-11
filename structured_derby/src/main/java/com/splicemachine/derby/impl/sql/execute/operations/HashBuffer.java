package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

public class HashBuffer<K,T extends ExecRow> extends LinkedHashMap<K,T>{
	private static final long serialVersionUID = -191068765821075562L;
	protected int maxCapacity;
	protected Entry<K,T> eldestEntry;
	protected boolean evicted;
	protected T matchedRow = null;
	
	public HashBuffer(int maxCapacity){
		super(0, 0.75F,true); // LRU CACHE
		this.maxCapacity = maxCapacity;
	}
	
	@Override
	protected boolean removeEldestEntry(Entry<K, T> eldest) {
		evicted =  size() >= this.maxCapacity;
		if (evicted)
			this.eldestEntry = eldest;
		return evicted;
	}
	
	public T add(K key, T element){
		put(key,element);
		if (evicted)
			return eldestEntry.getValue();
		return element;
	}
	
	public boolean merge(K key, T element, Merger<K,T> merger){
		if ( (matchedRow = merger.shouldMerge(key)) == null) {
			return false;
		}
		merger.merge(matchedRow, element);
		return true;
	}
	
	public HashBuffer<K,T> finishAggregates(AggregateFinisher<K,T> aggregateFinisher) throws StandardException {
		HashBuffer<K,T> finalizedBuffer = new HashBuffer<K,T>(maxCapacity);
		for (Entry<K,T> entry: entrySet()) {
			finalizedBuffer.put(entry.getKey(), aggregateFinisher.finishAggregation(entry.getValue()));
		}
		this.clear();
		return finalizedBuffer;
	}
	
	@Override
	public String toString(){
		return "RingBuffer{maxSize="+maxCapacity+",LinkedHashMap="+super.toString()+"}";
	}
	public interface Merger<K,T extends ExecRow> {
		void merge(T currentRow, T nextRow);
		T shouldMerge(K key);
	}

	public interface AggregateFinisher<K, T extends ExecRow> {
		T finishAggregation(T row) throws StandardException;
	}
}
