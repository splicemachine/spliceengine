package com.splicemachine.si.impl;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;

public class CacheReference extends SoftReference {
    final Object key;

    public CacheReference(Object referent, ReferenceQueue q, Object key) {
        super(referent, q);
        this.key = key;
    }
}
