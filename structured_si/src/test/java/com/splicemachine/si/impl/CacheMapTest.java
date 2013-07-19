package com.splicemachine.si.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CacheMapTest {
    @Test
    public void testPutGet() {
        final Map cache = CacheMap.makeCache(false, 3);
        cache.put(1, 10);
        Assert.assertEquals(10, cache.get(1));
    }

    @Test
    public void testCannotPutPastMaxSize() {
        final Map cache = CacheMap.makeCache(false, 3);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.put(4, 40);
        Assert.assertEquals(30, cache.get(3));
        Assert.assertNull(cache.get(4));
    }

    @Test
    public void testCanPutPastMaxSizeAfterRemove() {
        final Map cache = CacheMap.makeCache(false, 3);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.remove(2);
        cache.put(4, 40);
        Assert.assertEquals(30, cache.get(3));
        Assert.assertEquals(40, cache.get(4));
    }

    @Test
    public void testPutGetShared() {
        final Map cache = CacheMap.makeCache(true, 3);
        cache.put(1, 10);
        Assert.assertEquals(10, cache.get(1));
    }

    @Test
    public void testCannotPutPastMaxSizeShared() {
        final Map cache = CacheMap.makeCache(true, 3);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.put(4, 40);
        Assert.assertEquals(30, cache.get(3));
        Assert.assertNull(cache.get(4));
    }

    @Test
    public void testCanPutPastMaxSizeAfterRemoveShared() {
        final Map cache = CacheMap.makeCache(true, 3);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30);
        cache.remove(2);
        cache.put(4, 40);
        Assert.assertEquals(30, cache.get(3));
        Assert.assertEquals(40, cache.get(4));
    }
}
