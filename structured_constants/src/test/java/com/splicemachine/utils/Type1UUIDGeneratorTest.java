package com.splicemachine.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 2/27/14
 */
public class Type1UUIDGeneratorTest {
		@Test
		public void testCanGenerateUUIDsInRecurringBlocksWithoutDuplication() throws Exception {
				UUIDGenerator generator = Type1UUIDGenerator.instance();
				Set<byte[]> uuids = Sets.newTreeSet(Bytes.BYTES_COMPARATOR);
				for(int i=0;i<2*Short.MAX_VALUE;i++){
						byte[] next = generator.nextBytes();
						Assert.assertFalse("uuid " + next + " already seen", uuids.contains(next));
						uuids.add(next);
				}
		}

		@Test
		public void testNoDuplicatesManyThreadsSameJVM() throws Exception {
				int numThreads=10;
				final int numIterations = 160000;
				ExecutorService executor = Executors.newFixedThreadPool(numThreads);
				final ConcurrentMap<byte[],Boolean> existing = new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);
				List<Future<Boolean>> futures = Lists.newArrayListWithCapacity(numThreads);
				final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);
				for(int i=0;i<numThreads;i++){
						futures.add(executor.submit(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
										startBarrier.await(); //wait for everyone to force contention
										UUIDGenerator generator = Type1UUIDGenerator.instance();
										for(int i=0;i<numIterations;i++){
												long time = System.currentTimeMillis();
												byte[] uuid = generator.nextBytes();
												if(existing.putIfAbsent(uuid,true)!=null){
														System.out.printf("     already present!i=%d,value=%s, time=%d,count=%d%n",
																		i, Bytes.toStringBinary(uuid), time, Bytes.toShort(uuid, 14));
//														System.out.println("     already present!i= "+i+", value="+Bytes.toStringBinary(uuid)+", time="+Long.toBinaryString(time));
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
						testNoDuplicatesManyThreadsSameJVM();
				}
		}
}
