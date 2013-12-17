package com.splicemachine.si.impl;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

public class CacheReference<K, V> extends SoftReference<V> {
    final Object key;

    public CacheReference(V referent, ReferenceQueue<V> q, K key) {
        super(referent, q);
        this.key = key;
    }
}
