package com.splicemachine.derby.impl.job.scheduler;

import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Date: 12/2/13
 */
abstract class BasicPlacementStrategy implements TerracedQueue.PlacementStrategy<Runnable>,ThreadFactory{
		private final int size;
		private final AtomicInteger threadCount = new AtomicInteger(0);
		private final boolean daemonThreads;
		private final Thread.UncaughtExceptionHandler uncaughtExceptionHandler;
		private final int threadPriority;

		private final int[] activeThreadsByTerrace;
		private final TerracedQueue.TerraceSizingStrategy sizingStrategy;

		protected BasicPlacementStrategy(int size,
																		 TerracedQueue.TerraceSizingStrategy sizingStrategy,
																		 Thread.UncaughtExceptionHandler uncaughtExceptionHandler){
				this(size,sizingStrategy,false,uncaughtExceptionHandler,Thread.NORM_PRIORITY);
		}

		protected BasicPlacementStrategy(int size,TerracedQueue.TerraceSizingStrategy sizingStrategy) {
				this(size,sizingStrategy,false,null,Thread.NORM_PRIORITY);
		}

		protected BasicPlacementStrategy(int size, TerracedQueue.TerraceSizingStrategy sizingStrategy,
																		 boolean daemonThreads) {
				this(size,sizingStrategy,daemonThreads,null,Thread.NORM_PRIORITY);
		}

		protected BasicPlacementStrategy(int size,
																		 TerracedQueue.TerraceSizingStrategy sizingStrategy,
																		 boolean daemonThreads,
																		 Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
				this(size,sizingStrategy,daemonThreads,uncaughtExceptionHandler,Thread.NORM_PRIORITY);
		}

		protected BasicPlacementStrategy(int size,
																		 TerracedQueue.TerraceSizingStrategy sizingStrategy,
																		 boolean daemonThreads,
																		 Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
																		 int threadPriority) {
				this.size = size;
				this.sizingStrategy = sizingStrategy;
				this.daemonThreads = daemonThreads;
				this.uncaughtExceptionHandler = uncaughtExceptionHandler;
				this.threadPriority = threadPriority;

				this.activeThreadsByTerrace = new int[size];
				Arrays.fill(activeThreadsByTerrace, 0);
		}


		@Override
		public int getAssignedTerrace(Thread thread) {
				if(thread instanceof TerracedThread) return ((TerracedThread)thread).terrace;
				return size/2;  //somewhere in the middle of the priority list by default
		}

		@Override
		public Runnable transform(Runnable item, int terrace) {
				return new TerracedRunnable(item,terrace);
		}

		@Override
		public synchronized Thread newThread(Runnable r) {
				/*
				 * Get the highest terrace less than or equal to the found terrace which has
				 * an open number of threads, and assign it there
				 *
				 * We use a TerracedRunnable concept to ensure that the thread that is created
				 * is assigned to the correct terrace. However, there isn't a guarantee that the
				 * runnable will have been queued before execution, in which case we'll need to get
				 * the assigned terrace for the runnable directly.
				 */
				int terrace;
				if(r instanceof TerracedRunnable)
					terrace = ((TerracedRunnable)r).terrace;
				else
					terrace = assignTerrace(r);

				while(terrace>=0 && activeThreadsByTerrace[terrace]>=sizingStrategy.getSize(terrace)){
						terrace--;
				}
				//indicate that this pool is full
				activeThreadsByTerrace[terrace]++;

				String name = String.format("terrace-%d-thread-%d",terrace,threadCount.incrementAndGet());
				TerracedThread t =  new TerracedThread(r,terrace);
				t.setName(name);
				t.setDaemon(daemonThreads);
				if(uncaughtExceptionHandler!=null)
						t.setUncaughtExceptionHandler(uncaughtExceptionHandler);
				t.setPriority(threadPriority);

				return t;
		}

		private class TerracedThread extends Thread{
				private final int terrace;

				private TerracedThread(Runnable target,int terrace) {
						super(target);
						this.terrace = terrace;
				}

				@Override
				public void run() {
						try{
								super.run();
						}finally{
								synchronized (BasicPlacementStrategy.this){
									activeThreadsByTerrace[terrace]--;
								}
						}
				}
		}

		private class TerracedRunnable implements Runnable {
				private final Runnable item;
				private final int terrace;

				public TerracedRunnable(Runnable item, int terrace) {
						this.item = item;
						this.terrace = terrace;
				}

				@Override
				public void run() {
						item.run();
				}
		}
}
