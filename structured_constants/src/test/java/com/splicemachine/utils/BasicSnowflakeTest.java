package com.splicemachine.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 6/21/13
 */
//@Ignore
public class BasicSnowflakeTest {
    @Test
    public void testcanCreateAUUID() throws Exception {
        Snowflake snowflake = new Snowflake((short)(1<<6));
        long uuid = snowflake.nextUUID();
        Assert.assertTrue(uuid != 0); //uuids should never == 0 unless the tiemstamp is all kinds of off.
    }

    @Test
    public void testNoDuplicateUUIDsInASingleThread() throws Exception {
        Set<Long> uuidSet = Sets.newTreeSet();
        Snowflake snowflake = new Snowflake((short)(1<<7));
        for(int i=0;i<100000;i++){
            long uuid = snowflake.nextUUID();
            Assert.assertFalse("duplicate uuid found!",uuidSet.contains(uuid));
            uuidSet.add(uuid);
        }
    }

    @Test
    public void testNoDuplicateUUIDsInASingleThreadBytes() throws Exception {
        Set<byte[]> uuidSet = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
        Snowflake snowflake = new Snowflake((short)(1<<7));
        for(int i=0;i<1000000;i++){
            byte[] uuid = snowflake.nextUUIDBytes();
            Assert.assertFalse("duplicate uuid found!",uuidSet.contains(uuid));
            uuidSet.add(uuid);
        }
    }

    @Test
    public void testNoDuplicatesManyThreadsSameSnowflake() throws Exception {
        int numThreads=3;
        final int numIterations = 100000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final ConcurrentMap<Long,Boolean> existing = new ConcurrentHashMap<Long, Boolean>();
        List<Future<Boolean>> futures = Lists.newArrayListWithCapacity(numThreads);
        final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);
        final Snowflake snowflake = new Snowflake((short)((1<<11)+1));
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    startBarrier.await(); //wait for everyone to force contention
                    for(int i=0;i<numIterations;i++){
                        long time = System.currentTimeMillis();
                        long uuid = snowflake.nextUUID();
                        if(existing.putIfAbsent(uuid,true)!=null){
                            System.out.println("     already present!i= "+i+", value="+Long.toBinaryString(uuid)+", time="+Long.toBinaryString(time));
                            return false;  //uh-oh, duplicates!
                        }
                    }
                    return true;
                }
            }));
        }

        startBarrier.await(); //tell everyone to start
        for(Future<Boolean> future:futures){
            Assert.assertTrue("Duplicate entry found!",future.get());
        }
        //make sure that the correct number of uuids were generated
        Assert.assertEquals("Incorrect number of uuids generated!",numThreads*numIterations,existing.size());
    }

    @Test
    public void testRepeatedNoDuplicatesManyThreadsSameSnowflake() throws Exception {
        for(int i=0;i<10;i++){
            System.out.println("Trying iteration "+ i);
            testNoDuplicatesManyThreadsSameSnowflake();
        }
    }

    @Test
    public void testRepeatedNoDuplicatesManyThreads() throws Exception {
        for(int i=0;i<10;i++){
            System.out.println("Trying iteration "+ i);
            testNoDuplicatesManyThreads();
        }
    }

    @Test
    public void testNoDuplicatesManyThreads() throws Exception {
        int numThreads=5;
        final int numIterations = 200000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final ConcurrentMap<Long,Boolean> existing = new ConcurrentHashMap<Long, Boolean>();
        List<Future<Boolean>> futures = Lists.newArrayListWithCapacity(numThreads);
        final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);

        final AtomicInteger counter = new AtomicInteger(0);
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    short nextMachineId = (short)(counter.incrementAndGet());
                    Snowflake snowflake = new Snowflake(nextMachineId);
                    startBarrier.await(); //wait for everyone to force contention
                    for(int i=0;i<numIterations;i++){
                        long uuid = snowflake.nextUUID();
                        if(existing.putIfAbsent(uuid,true)!=null){
                            System.out.println("     already present!i= "+i+", value="+Long.toBinaryString(uuid)+", time="+Long.toBinaryString(System.currentTimeMillis()));
                            return false;  //uh-oh, duplicates!
                        }
                    }
                    return true;
                }
            }));
        }

        startBarrier.await(); //tell everyone to start
        for(Future<Boolean> future:futures){
            Assert.assertTrue("Duplicate entry found!",future.get());
        }
        //make sure that the correct number of uuids were generated
        Assert.assertEquals("Incorrect number of uuids generated!",numThreads*numIterations,existing.size());
    }
}
