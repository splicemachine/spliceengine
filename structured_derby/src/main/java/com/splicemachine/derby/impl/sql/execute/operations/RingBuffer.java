package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RingBuffer<T> implements Iterable<T>{
	public interface Merger<T> {
		void merge(T one, T two);
		boolean shouldMerge(T one, T two);
	}
	
	private final List<T> elements;
	private final int maxSize;
	private int pos;
	
	public RingBuffer(int size){
		elements = new ArrayList<T>(size);
		this.maxSize = size;
		this.pos = 0;
	}
	
	public T add(T element){
		T retEl = element;
		pos = (pos+1)%maxSize;
		if(pos >= elements.size()) elements.add(element);
		else
			retEl = elements.set(pos, element);
		return retEl;
	}
	
	public boolean merge(T element, Merger<T> merger){
		for(T e: elements){
			if(merger.shouldMerge(e, element)){
				merger.merge(e,element);
				return true;
			}
		}
		return false;
	}
	
	@Override
	public Iterator<T> iterator(){
		return elements.iterator();
	}
	
	public void clear(){
		elements.clear();
	}
	
	@Override
	public String toString(){
		return "RingBuffer{maxSize="+maxSize+",pos="+pos+",elements="+elements.toString()+"}";
	}
}
