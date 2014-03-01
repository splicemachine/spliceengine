package com.splicemachine.si;

import java.util.concurrent.ConcurrentHashMap;
/*
 * This class exists as an approach to capture existing transactions without having to 
 * clean out a hashmap in the case of a single transaction 
 *
 */
public class SpliceReusableHashmap<K,V>  {
	int count = 0;
	V singleReferenceValue;
	K singleReferenceKey;
	ConcurrentHashMap<K,V> map = new ConcurrentHashMap<K,V>();
	
	public void reset() {
		count = 0;
	}
	
	public boolean contains(K key) {
		if (count == 1)
			return singleReferenceKey==key;
		if (count == 2)
			return map.contains(key);
		return false;
	}
	
	public void add(K key, V value) {
		count++;
		if (count ==1) {
			singleReferenceValue = value;
			singleReferenceKey = key;
		}
		else {
			if (count ==2) {
				map.clear();
				map.put(singleReferenceKey, singleReferenceValue);
			}
			map.put(key, value);
		} 
	}
	
	public V get(K key) {
		if (count ==0)
			return null;
		else if (count ==1 && singleReferenceKey.equals(key))
			return singleReferenceValue;
		else {
			return map.get(key);
		}
	}
	public void close() {
		singleReferenceValue = null;
		map.clear();
		map = null;
	}
}
