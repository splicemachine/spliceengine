package com.splicemachine.si.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.concurrent.ThreadLocalRandom;
import com.splicemachine.si.api.TransactionTimeoutException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.coprocessors.TxnLifecycleProtocol;
import com.splicemachine.utils.ThreadSafe;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 6/25/14
 */
public class QueuedKeepAliveScheduler implements KeepAliveScheduler {
		private static final Logger LOG = Logger.getLogger(QueuedKeepAliveScheduler.class);
		private final long maxWaitIntervalMs;
		private final long maxKeepAliveIntervalMs;

		private final ScheduledExecutorService threadPool;
		private final com.splicemachine.concurrent.ThreadLocalRandom random;

		private final @ThreadSafe TxnLifecycleProtocol lifecycleProtocol;

		private volatile boolean shutdown = false;


		public QueuedKeepAliveScheduler(long maxWaitIntervalMs, long maxKeepAliveIntervalMs,
																		int numKeepers, TxnLifecycleProtocol lifecycleProtocol) {
				this.maxWaitIntervalMs = maxWaitIntervalMs;
				ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("keepAlive-thread-%d").setDaemon(true).build();

				this.threadPool = Executors.newScheduledThreadPool(numKeepers,factory);
				this.random = ThreadLocalRandom.current();
				this.lifecycleProtocol = lifecycleProtocol;
				this.maxKeepAliveIntervalMs = maxKeepAliveIntervalMs;
		}

		@Override
		public void scheduleKeepAlive(Txn txn) {
				if(shutdown) return;

//				activeTxns.add(txn);
				threadPool.schedule(new KeepAlive(txn), random.nextLong(maxWaitIntervalMs), TimeUnit.MILLISECONDS);
		}

		@Override
		public void start() {
		}

		@Override
		public void stop() {
				shutdown = true;
				threadPool.shutdownNow();
		}

		private class KeepAlive implements Runnable{
				private final Txn txn;
				private long lastKeepAliveTime;

				public KeepAlive(Txn txn) {
						this.txn = txn;
						this.lastKeepAliveTime = System.currentTimeMillis();
				}

				@Override
				public void run() {
						if(txn.getEffectiveState()!= Txn.State.ACTIVE){
								return; //nothing to do, we no longer need to keep anything alive
						}
						long keepAliveTime = System.currentTimeMillis()-lastKeepAliveTime;

						if(keepAliveTime>2*maxKeepAliveIntervalMs){
								/*
								 * We are the only ones trying to keep this transaction alive. If we know
								 * for a fact that we had to wait longer than the transaction timeout, then
								 * we don't need to keep trying--just roll back the transaction and return.
								 *
								 * However, we want to leave some room for network slop here, so we err
								 * on the side of caution, and only use this if we exceed twice the actual
								 * keep alive window. That way, we probably never need this, but it's available
								 * if we do.
								 */
								try{
										txn.rollback();
								} catch (IOException e) {
										LOG.info("Unable to roll back transaction " +
														txn.getTxnId() + " but nothing to be concerned with, since it has already timed out", e);
								}
								return;
						}

						try {
								boolean reschedule = lifecycleProtocol.keepAlive(txn.getTxnId());
								if(reschedule){
										//use a random slop factor to load-balance our keep alive requests.
										threadPool.schedule(this,random.nextLong(maxWaitIntervalMs),TimeUnit.MILLISECONDS);
										lastKeepAliveTime = System.currentTimeMillis(); //include network latency in our wait period
								}
						} catch(TransactionTimeoutException tte){
									/*
									 * We attempted to keep alive a transaction that has already timed out for a different
									 * reason. Ensure that the transaction is rolled back
									 */
								try {
										txn.rollback();
								} catch (IOException e) {
										LOG.info("Unable to roll back transaction "+
														txn.getTxnId()+" but nothing to be concerned with, since it has already timed out",e);
								}
						} catch (IOException e) {
								/*
								 * This could be a real problem, but we don't have anything that we can really do about this,
								 * so we just log the error and hope it resolves itself.
								 */
								LOG.error("Unable to keep transaction "+txn.getTxnId()+" alive. Will try again in a bit");
								threadPool.schedule(this,random.nextLong(maxWaitIntervalMs),TimeUnit.MILLISECONDS);
						}
				}
		}
}
