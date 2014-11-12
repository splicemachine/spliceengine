package com.splicemachine.tools;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Created on: 9/18/13
 */
public class LongHashMapTest {
    @Test
    public void testCanPutAndGet() throws Exception {
        LongHashMap<Integer> test = LongHashMap.evictingMap(10);
        test.put(1l,1);
        Assert.assertEquals(1,(int)test.get(1l));
    }

    @Test
    public void testEvictionEvictsEntries() throws Exception {
        Map<Long,Integer> evicted = Maps.newHashMap();
        LongHashMap<Integer> test = LongHashMap.evictingMap(10);
        for(int i=0;i<20;i++){
            LongHashMap.LongEntry<Integer> put = test.put((long) i, i);
            if(put!=null){
                evicted.put(put.getKey(),put.getValue());
            }
        }
        Assert.assertEquals(4,evicted.size());

        //make sure that evicted keys aren't present any more
        for(Long evictedKey:evicted.keySet()){
            Assert.assertNull(test.get(evictedKey));
        }

        //make sure that anything in the map hasn't been evicted
        for(int i=0;i<10;i++){
            long k = (long)i;
            if(test.get(k)!=null){
                Assert.assertFalse(evicted.containsKey(k));
            }
        }
    }
}
