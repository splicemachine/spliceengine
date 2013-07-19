package com.splicemachine.si.impl;

import java.lang.ref.ReferenceQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Intended to be lightweight alternative to a google cache. Holds cache values (but not keys) as soft references
 * to avoid consuming too much heap. Will not store more than maxSize items to avoid consuming too much heap. Optionally,
 * can be thread safe for concurrent access.
 */
public class CacheMap implements Map {
    final Map<Object, CacheReference> delegate;
    final ReferenceQueue referenceQueue;
    final int maxSize;
    final static int DEFAULT_MAX_SIZE = 10000;

    /**
     * Factory function for creating a cache. Uses the default size.
     */
    public static Map makeCache(boolean shared) {
        return makeCache(shared, DEFAULT_MAX_SIZE);
    }

    /**
     * Use this factory function for creating instances.
     *
     * @param shared indicates whether the cache needs to be thread safe
     * @param maxSize the maximum number of items the cache can contain, items added past this limit are ignored
     * @return new cache object
     */
    public static Map makeCache(boolean shared, int maxSize) {
        return new CacheMap(shared ? new ConcurrentHashMap() : new HashMap(), maxSize);
    }

    private CacheMap(Map delegate, int maxSize) {
        this.referenceQueue = new ReferenceQueue<CacheReference>();
        this.delegate = delegate;
        this.maxSize = maxSize;
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        CacheReference reference = delegate.get(key);
        if (reference == null) {
            return null;
        } else {
            Object result = reference.get();
            if (result == null) {
                delegate.remove(key);
            }
            return result;
        }
    }

    @Override
    public Object put(Object key, Object value) {
        purge();
        if (delegate.size() < maxSize) {
            return delegate.put(key, new CacheReference(value, referenceQueue, key));
        } else {
            return get(key);
        }
    }

    private void purge() {
        if (delegate.size() >= maxSize / 2) {
            for (CacheReference ref = (CacheReference) referenceQueue.poll(); ref != null; ref = (CacheReference) referenceQueue.poll()) {
                delegate.remove(ref.key);
            }
        }
    }

    @Override
    public Object remove(Object key) {
        return delegate.remove(key);
    }

    @Override
    public void putAll(Map m) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public Set keySet() {
        return delegate.keySet();
    }

    @Override
    public Collection values() {
        return delegate.values();
    }

    @Override
    public Set<Entry> entrySet() {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }
}
