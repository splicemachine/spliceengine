package com.splicemachine.collections;


import com.google.common.cache.CacheStats;
import com.splicemachine.hash.HashFunctions;
import org.junit.Assert;
import org.junit.Test;

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
}
