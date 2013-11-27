package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectOpenHashMap;

public class LongPrimitiveCacheMap<T> extends LongObjectOpenHashMap<T> {
	LongArrayList referenceQueue;
	final int maxSize;
	final static int DEFAULT_MAX_SIZE = 10000;

public LongPrimitiveCacheMap() {
	this(DEFAULT_MAX_SIZE);
}
	
public LongPrimitiveCacheMap(int maxSize) {
    this.referenceQueue = new LongArrayList();
    this.maxSize = maxSize;
}

@Override
public T put(long key, T value) {
	if (referenceQueue.size() >= maxSize)
		remove(referenceQueue.remove(0));
	referenceQueue.add(key);
	return super.put(key,value);
}

}
