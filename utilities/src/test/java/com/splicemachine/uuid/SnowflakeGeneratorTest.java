/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.uuid;

import com.splicemachine.uuid.Snowflake;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 9/24/13
 */
public class SnowflakeGeneratorTest {

    @Test
    public void testCanGenerateUUIDsInRecurringBlocksWithoutDuplication() throws Exception {
        Snowflake snowflake = new Snowflake((short)1);
        Snowflake.Generator generator = snowflake.newGenerator(10);
        Set<Long> uuids = new HashSet<Long>();
        for(int i=0;i<21;i++){
            long next = generator.next();
            Assert.assertFalse("uuid "+ next+" already seen",uuids.contains(next));
            uuids.add(next);
        }
    }

    @Test
    public void testNoDuplicatesManyThreadsSameSnowflakeDifferentGenerators() throws Exception {
        int numThreads=10;
        final int numIterations = 16000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final ConcurrentMap<Long,Boolean> existing = new ConcurrentHashMap<Long, Boolean>();
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(numThreads);
        final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);
        final Snowflake snowflake = new Snowflake((short)((1<<11)+1));
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    startBarrier.await(); //wait for everyone to force contention
                    Snowflake.Generator generator = snowflake.newGenerator(100);
                    for(int i=0;i<numIterations;i++){
                        long time = System.currentTimeMillis();
                        long uuid = generator.next();
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
            testNoDuplicatesManyThreadsSameSnowflakeDifferentGenerators();
        }
    }

    @Test
    public void testRepeatedNoDuplicatesManyThreads() throws Exception {
        for(int i=0;i<10;i++){
            testNoDuplicatesManyThreads();
        }
    }

    @Test
    public void testNoDuplicatesManyThreads() throws Exception {
        int numThreads=5;
        final int numIterations = 20000;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        final ConcurrentMap<Long,Boolean> existing = new ConcurrentHashMap<Long, Boolean>();
        List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(numThreads);
        final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);

        final AtomicInteger counter = new AtomicInteger(0);
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    short nextMachineId = (short)(counter.incrementAndGet());
                    Snowflake snowflake = new Snowflake(nextMachineId);
                    Snowflake.Generator generator = snowflake.newGenerator(100);
                    startBarrier.await(); //wait for everyone to force contention
                    for(int i=0;i<numIterations;i++){
                        long uuid = generator.next();
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
