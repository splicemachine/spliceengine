package com.splicemachine.derby.impl.job.scheduler;

import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 12/2/13
 */
public class TerracedThreadPool extends ThreadPoolExecutor{

		public TerracedThreadPool(int corePoolSize,
															int maximumPoolSize,
															long keepAliveTime,
															TimeUnit unit,
															TerracedQueue.PlacementStrategy<Runnable> placementStrategy,
															TerracedQueue.TerraceSizingStrategy sizingStrategy ){
				super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
								new TerracedQueue<Runnable>(placementStrategy,sizingStrategy));
		}

		public TerracedThreadPool(int corePoolSize,
															int maximumPoolSize,
															long keepAliveTime,
															TimeUnit unit,
															TerracedQueue.PlacementStrategy<Runnable> placementStrategy,
															TerracedQueue.TerraceSizingStrategy sizingStrategy,
															ThreadFactory threadFactory) {
				super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
								new TerracedQueue<Runnable>(placementStrategy,sizingStrategy), threadFactory);
		}

		public TerracedThreadPool(int corePoolSize,
															int maximumPoolSize,
															long keepAliveTime,
															TimeUnit unit,
															TerracedQueue.PlacementStrategy<Runnable> placementStrategy,
															TerracedQueue.TerraceSizingStrategy sizingStrategy,
															RejectedExecutionHandler handler) {
				super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
								new TerracedQueue<Runnable>(placementStrategy,sizingStrategy), handler);
		}

		public TerracedThreadPool(int corePoolSize,
															int maximumPoolSize,
															long keepAliveTime,
															TimeUnit unit,
															TerracedQueue.PlacementStrategy<Runnable> placementStrategy,
															TerracedQueue.TerraceSizingStrategy sizingStrategy,
															ThreadFactory threadFactory,
															RejectedExecutionHandler handler) {
				super(corePoolSize, maximumPoolSize, keepAliveTime, unit,
								new TerracedQueue<Runnable>(placementStrategy,sizingStrategy), threadFactory, handler);
		}

		@Override
		protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
				return new CallableAccessibleFutureTask<T>(callable);
		}


}
