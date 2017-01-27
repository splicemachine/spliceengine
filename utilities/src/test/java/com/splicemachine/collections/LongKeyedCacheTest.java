/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.collections;


import org.spark_project.guava.cache.CacheStats;
import com.splicemachine.hash.HashFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Tests for the LongKeyedCache's correctness in a single thread.
 *
 * @author Scott Fines
 * Date: 9/22/14
 */
public class LongKeyedCacheTest {

    @Test
    public void testCanPutAndThenFetchFromEmptyCache() throws Exception {
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(4).build();

        cache.put(1l,1l);

        Assert.assertEquals("incorrect size estimate!",1,cache.size());
        Long elem = cache.get(1l);
        Assert.assertEquals("Incorrect cache fetch!",1l,elem.longValue());
    }

    @Test
    public void testPuttingSameElementInTwiceDoesNotDuplicateEntries() throws Exception {
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(4).build();

        cache.put(1l,1l);

        Assert.assertEquals("incorrect size estimate!",1,cache.size());
        cache.put(1l,1l);

        Assert.assertEquals("incorrect size estimate!",1,cache.size());

        Long elem = cache.get(1l);
        Assert.assertEquals("Incorrect cache fetch!",1l,elem.longValue());
    }

    @Test
    public void testHashConflictsStillFindableWithSoftReferences() throws Exception {
        int size = 8;
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(size).withSoftReferences().build();

        for(long i=0;i<1024;i++){
            long e = i;
            cache.put(e,e);
            //ensure that I can still get that element out
            Long elem = cache.get(e);
            Assert.assertEquals("Incorrect cache fetch!",e,elem.longValue());

            if(i<size)
                Assert.assertEquals("Cache size is incorrect!",i+1,cache.size());
            else
                Assert.assertEquals("Cache size is incorrect!",size,cache.size());

        }
    }

    @Test
    public void testHashConflictsStillFindable() throws Exception {
        int size = 8;
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(size).build();

        for(long i=0;i<1024;i++){
            long e = i;
            cache.put(e,e);
            //ensure that I can still get that element out
            Long elem = cache.get(e);
            Assert.assertEquals("Incorrect cache fetch!", e, elem.longValue());

            if(i<size)
                Assert.assertEquals("Cache size is incorrect!",i+1,cache.size());
            else
                Assert.assertEquals("Cache size is incorrect!",size,cache.size());

        }
    }

    @Test
    public void testCannotFindMissingElementAfterConflicts() throws Exception {
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(4).build();

        for(long i=0;i<10;i++){
            long e = (1<<i);
            cache.put(e,e);
            if(i<4)
                Assert.assertEquals("Cache size is incorrect!",i+1,cache.size());
            else
                Assert.assertEquals("Cache size is incorrect!",4,cache.size());

            //ensure that I can still get that element out
            Long elem = cache.get(e);
            Assert.assertEquals("Incorrect cache fetch!",e,elem.longValue());
            Long missing = cache.get(e+1);
            Assert.assertNull("Found a non-existent entry!",missing);
        }
    }

    @Test
    public void testEvictsEntriesAfterFilling() throws Exception {
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(4).withHashFunction(HashFunctions.murmur3(0)).build();

        for(long i=0;i<10;i++){
            cache.put(i,i);
            if(i<4)
                Assert.assertEquals("Cache size is incorrect!",i+1,cache.size());
            else
                Assert.assertEquals("Cache size is incorrect!", 4, cache.size());

            //ensure that I can still get that element out
            Long elem = cache.get(i);
            Assert.assertEquals("Incorrect cache fetch!",i,elem.longValue());
        }
    }

    @Test
    public void testCacheStatsWorks() throws Exception {
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(4).collectStats().build();

        for(long i=0;i<10;i++){
            cache.put(i,i);
            if(i<4)
                Assert.assertEquals("Cache size is incorrect!",i+1,cache.size());
            else
                Assert.assertEquals("Cache size is incorrect!", 4, cache.size());

            //ensure that I can still get that element out
            Long elem = cache.get(i);
            Assert.assertEquals("Incorrect cache fetch!",i,elem.longValue());
        }

        CacheStats stats = cache.getStats();
        Assert.assertEquals("Incorrect hit count!",10l,stats.hitCount());
        Assert.assertEquals("Incorrect miss count!",0l,stats.missCount());
        Assert.assertEquals("Incorrect request count!",10l,stats.requestCount());
        Assert.assertEquals("Incorrect eviction count!",6l,stats.evictionCount());
    }

    @Test
    public void testEviction() {
        final int maxElements = 8;
        final int iterations = 1024;
        LongKeyedCache<Long> cache = LongKeyedCache.<Long>newBuilder().maxEntries(maxElements)
                .withHashFunction(HashFunctions.murmur3(0)).build();


        int count = 0;

        Random rand = new Random(0);
        for (int i = 0; i < iterations; ++i) {
            cache.put(count++, rand.nextLong());
            Assert.assertTrue("There has been a shortcircuit on the linked list", count(cache) >= Math.min(i, maxElements));
        }

        for (int batchSize = 0; batchSize <= 2 * maxElements; ++batchSize) {
            int misses = 0;
            int hits = 0;

            int[] values = new int[batchSize];
            for (int i = 0; i < iterations; ++i) {
                for (int k = 0; k < batchSize; k++) {
                    values[k] = count++;
                }
                for (int k = 0; k < batchSize; k++) {
                    cache.put(values[k], rand.nextLong());
                    Assert.assertTrue("There has been a shortcircuit on the linked list", count(cache) >= maxElements);
                }

                for (int k = 0; k < batchSize; k++) {
                    if (cache.get(values[k]) == null) {
                        misses++;
                    } else {
                        hits++;
                    }
                }
            }

            if (batchSize <= maxElements) {
                Assert.assertEquals("Unexpected cache misses", 0, misses);
                Assert.assertEquals("Missing hits", iterations * batchSize, hits);
            } else {
                int expectedMisses = (batchSize - maxElements)*iterations;
                Assert.assertEquals("Wrong cache misses", expectedMisses, misses);
                Assert.assertEquals("Wrong cache hits", iterations * batchSize - expectedMisses, hits);
            }
        }
    }

    private int count(LongKeyedCache<?> cache) {
        int count = 0;
        LongKeyedCache.Holder head = cache.head;
        while (head != null) {
            count++;
            head = head.next;
        }
        return count;
    }
}
