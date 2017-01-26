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

package com.splicemachine.uuid;

import com.splicemachine.primitives.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 2/27/14
 */
public class Type1UUIDGeneratorTest {
//		@Test
//		public void testCanGenerateUUIDsInRecurringBlocksWithoutDuplicationLots() throws Exception {
//				UUIDGenerator generator = Type1UUID.newGenerator(128);
//				Set<byte[]> uuids = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
//				for(int i=0;i<1000000;i++){
//						byte[] next = generator.nextBytes();
//						Assert.assertFalse(String.format("uuid %s already seen",Bytes.toStringBinary(next)), uuids.contains(next));
//						uuids.add(next);
//				}
//		}

		@Test
		public void testCanGenerateUUIDsInRecurringBlocksWithoutDuplication() throws Exception {
				UUIDGenerator generator = Type1UUID.newGenerator(128);
        Map<Long,Map<Long,Boolean>> uuids = new HashMap<Long, Map<Long, Boolean>>(){
            @Override
            public Map<Long, Boolean> get(Object key) {
                Map<Long, Boolean> longBooleanMap = super.get(key);
                if(longBooleanMap==null){
                    longBooleanMap = new HashMap<Long, Boolean>();
                    super.put((Long)key,longBooleanMap);
                }
                return longBooleanMap;
            }
        };
				for(int i=0;i<2*Short.MAX_VALUE;i++){
						byte[] next = generator.nextBytes();
            long high = Bytes.toLong(next);
            long low = Bytes.toLong(next,8);
            Map<Long, Boolean> longBooleanMap = uuids.get(high);
            Assert.assertFalse("uuidSet already contains entry " + high + "" + low, longBooleanMap.containsKey(low));
            uuids.get(high).put(low, Boolean.TRUE);
				}
		}

		@Test
		public void testNoDuplicatesManyThreadsSameJVM() throws Exception {
				int numThreads=10;
				final int numIterations = 160000;
				ExecutorService executor = Executors.newFixedThreadPool(numThreads);
				final ConcurrentMap<Long,ConcurrentMap<Long,Boolean>> existing
                = new ConcurrentHashMap<Long,ConcurrentMap<Long,Boolean>>();
				List<Future<Boolean>> futures = new ArrayList<Future<Boolean>>(numThreads);
				final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);
				for(int i=0;i<numThreads;i++){
						futures.add(executor.submit(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
										startBarrier.await(); //wait for everyone to force contention
										UUIDGenerator generator = Type1UUID.newGenerator(128);
										for(int i=0;i<numIterations;i++){
												long time = System.currentTimeMillis();
												byte[] uuid = generator.nextBytes();
                        long nextUuidHigh = Bytes.toLong(uuid);
                        long nextUuidLow = Bytes.toLong(uuid,8);

                        ConcurrentMap<Long,Boolean> entry = existing.get(nextUuidHigh);
                        if(entry==null){
                            ConcurrentMap<Long,Boolean> newMap = new ConcurrentHashMap<Long, Boolean>();
                            entry = existing.putIfAbsent(nextUuidHigh, newMap);
                            if(entry==null)
                                entry = newMap;
                        }
												if(entry.putIfAbsent(nextUuidLow,true)!=null){
                            String uuidStr = Long.toString(nextUuidHigh)+Long.toString(nextUuidLow);
														System.out.printf("     already present!i=%d,value=%s, time=%d,count=%d%n",
																		i, uuidStr, time, Bytes.toShort(uuid, 14));
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
        int totalSize = 0;
        for(Map.Entry<Long,ConcurrentMap<Long,Boolean>> entry:existing.entrySet()){
            totalSize+=entry.getValue().size();
        }

				//make sure that the correct number of uuids were generated
				Assert.assertEquals("Incorrect number of uuids generated!", numThreads * numIterations, totalSize);
		}


		@Test
		public void testRepeatedNoDuplicatedManyThreadsSameJVM() throws Exception {
				for(int i=0;i<10;i++){
						testNoDuplicatesManyThreadsSameJVM();
				}
		}
}
