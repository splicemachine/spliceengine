package com.splicemachine.tools;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 11/26/13
 */
public class MultithreadedOptimizingValveTest {
		private ExecutorService testService;

		int numThreads=4;

		@Before
		public void setUp() throws Exception {
				testService = Executors.newFixedThreadPool(numThreads);
		}

		@Test
		public void testMultipleThreadsCanAcquire() throws Exception {

				final Valve valve = new OptimizingValve(numThreads,numThreads,
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16),
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16));
				List<Future<Boolean>> futures = Lists.newArrayListWithCapacity(numThreads);
				for(int i=0;i<numThreads;i++){
						futures.add(testService.submit(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
										return valve.tryAllow() >=0;
								}
						}));
				}

				for(Future<Boolean> future:futures){
						Assert.assertTrue("Unable to acquire permits!", future.get());
				}
		}

		@Test
		public void testRepeatedSomeThreadsCannotAcquire() throws Exception {
				for(int i=0;i<100;i++){
						testSomeThreadsCannotAcquire();
				}
		}

		@Test
		public void testSomeThreadsCannotAcquire() throws Exception {
				final Valve valve = new OptimizingValve(numThreads,numThreads,
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16),
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16));
				List<Future<Integer>> futures = Lists.newArrayListWithCapacity(numThreads);
				for(int i=0;i<numThreads+1;i++){
						futures.add(testService.submit(new Callable<Integer>() {
								@Override
								public Integer call() throws Exception {
										return valve.tryAllow();
								}
						}));
				}

				int failCount =0;
				for(Future<Integer> future:futures){
						if(future.get()<0)
								failCount++;
				}
				Assert.assertEquals("Incorrect number of permits acquired!",1,failCount);
		}

		@Test
		public void testCanAdjustUpwardsConcurrently() throws Exception {
				final Valve valve = new OptimizingValve(numThreads,numThreads,
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16),
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16));
				final CyclicBarrier goodBarrier = new CyclicBarrier(numThreads+1);
				final CountDownLatch waitBarrier = new CountDownLatch(1);
				List<Future<Boolean>> futures = Lists.newArrayListWithCapacity(numThreads);
				for(int i=0;i<numThreads;i++){
						futures.add(testService.submit(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
										int version = valve.tryAllow();
										goodBarrier.await();
										if(version==0){
												System.out.printf("[%s] goodBarrier passed, returning version %d%n",
																Thread.currentThread().getName(), version);
												return version==0;
										}else{
												System.out.printf("[%s] Waiting for permit%n",Thread.currentThread().getName());
												waitBarrier.await();
												System.out.printf("[%s] Retrying acquisition%n", Thread.currentThread().getName());
												version = valve.tryAllow();
												System.out.printf("[%s] acquired, returning version %d%n",
																Thread.currentThread().getName(),version);
												return version==1;
										}
								}
						}));
				}

				goodBarrier.await();
				//now half the values are acquired --adjust upwards and then count down
				valve.adjustValve(Valve.SizeSuggestion.DOUBLE);
				waitBarrier.countDown();

				System.out.printf("[%s] Checking permit positions%n",Thread.currentThread().getName());
				for(Future<Boolean> future:futures){
						Assert.assertTrue("Incorrect permit position!",future.get());
				}

		}

		@Test(timeout=10000)
		public void testCanAdjustDownwardsConcurrently() throws Exception {
				final Valve valve = new OptimizingValve(numThreads,numThreads,
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16),
								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16));
				final CyclicBarrier barrierOne = new CyclicBarrier(numThreads+1);
				final CyclicBarrier barrierTwo = new CyclicBarrier(numThreads+1);
				final CyclicBarrier barrierThree = new CyclicBarrier(numThreads+1);
				final CountDownLatch waitBarrier = new CountDownLatch(1);
				List<Future<Boolean>> futures = Lists.newArrayListWithCapacity(numThreads);
				for(int i=0;i<numThreads;i++){
						futures.add(testService.submit(new Callable<Boolean>() {
								@Override
								public Boolean call() throws Exception {
										int version = valve.tryAllow();
										Assert.assertTrue("Unable to acquire initially!", version >= 0);

										System.out.printf("[%s] acquired initially%n",Thread.currentThread().getName());
										barrierOne.await();
										//release
										valve.release();
										System.out.printf("[%s] released%n", Thread.currentThread().getName());

										barrierTwo.await();

										version = valve.tryAllow();
										barrierThree.await();
										if(version>=1){
												System.out.printf("[%s] acquired, version=%d%n",Thread.currentThread().getName(),version);
												return version==1;
										}else{
												System.out.printf("[%s] waiting for resizing%n",Thread.currentThread().getName(),version);
												waitBarrier.await();
												version = valve.tryAllow();
												System.out.printf("[%s] acquired after wait, version=%d%n",Thread.currentThread().getName(),version);
												return version ==2;
										}
								}
						}));
				}

				//make sure everyone can acquire first
				barrierOne.await();

				//reduce the number of permits
				valve.adjustValve(Valve.SizeSuggestion.HALVE);

				//allow everyone to grab a permit
				barrierTwo.await();
				//make sure everyone is at the same point
				barrierThree.await();

				//now half the values are acquired --adjust upwards and allow them to pass
				valve.adjustValve(Valve.SizeSuggestion.DOUBLE);
				waitBarrier.countDown();

				System.out.printf("[%s] Checking permit positions%n",Thread.currentThread().getName());
				for(Future<Boolean> future:futures){
						Assert.assertTrue("Incorrect permit position!",future.get());
				}

		}
}
