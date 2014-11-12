package com.splicemachine.derby.hbase;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WriteSemaphore {
		public enum Status {
				DEPENDENT, INDEPENDENT, REJECTED
		}
//		private AtomicReference<WriteStatus> writeStatus = new AtomicReference<WriteStatus>(new WriteStatus(0,0,0,0));
		private final int maxDependentWriteThreads;
		private final int maxIndependentWriteThreads;
		private final int maxDependentWriteCount;
		private final int maxIndependentWriteCount;

		private final Semaphore dependentValve;
		private final Semaphore independentValve;
		private final AtomicInteger dependentRowCount = new AtomicInteger(0);
		private final AtomicInteger independentRowCount = new AtomicInteger(0);

		public WriteSemaphore(int maxDependentWriteThreads,
													int maxIndependentWriteThreads,
													int maxDependentWriteCount,
													int maxIndependentWriteCount) {
				assert (maxDependentWriteThreads>=0 &&
								maxIndependentWriteThreads >= 0 &&
								maxDependentWriteCount >= 0 &&
								maxIndependentWriteCount >= 0);
				this.dependentValve = new Semaphore(maxDependentWriteThreads);
				this.independentValve = new Semaphore(maxIndependentWriteThreads);
				this.maxIndependentWriteThreads  = maxIndependentWriteThreads;
				this.maxDependentWriteThreads = maxDependentWriteThreads;
				this.maxDependentWriteCount = maxDependentWriteCount;
				this.maxIndependentWriteCount = maxIndependentWriteCount;
		}

		public Status acquireDependentPermit(int writes) {
				/*
				 * The logic here is pretty simple:
				 *
				 * 1. try and acquire a permit for the dependent threads. If we can acquire
				 * a permit, then check if our dependent row count allows us to proceed. If it does,
				 * then we have reserved a slot. Otherwise, we release the thread permit and move on.
				 *
				 * We wait for up to 10 microseconds to see if we can get it. This should allow other
				 * threads which may have spuriously acquired the permit to perform their operations before
				 * releasing us, and is generally worth the wait. It does mean that it's possible for
				 * a write to wait 10 microseconds before being rejected, but that's fairly negligiable.
				 *
				 * We use microseconds because that's the resolution of most system clocks, so System.nanoTime()
				 * is likely to measure in it anyway.
				 */
				if (!acquireDependent()) return Status.REJECTED;
				//we have reserved a dependent thread. Now make sure that we have enough room
				if(!cappedIncrement(dependentRowCount,maxDependentWriteCount, writes)){
						//there are not enough slots available to ensure that we can write this row, we must reject
						//in order to reject, we must release the semaphore that we've acquired
						dependentValve.release();
						return Status.REJECTED;
				}
				return Status.DEPENDENT;
		}


		public boolean releaseDependentPermit(int writes) {
				//release the rows first
				dependentRowCount.addAndGet(-writes);
				//now release the semaphore
				dependentValve.release();
				return true;
		}

		public Status acquireIndependentPermit(int writes) {
				/*
				 * The logic here is a bit more complicated.
				 *
				 * We want it such that if we can acquire an INDEPENDENT permit, then
				 * we use those, but if not, we need to acquire a DEPENDENT permit. The natural
				 * approach would be to first attempt to acquire an independent permit, then if that
				 * fails acquire a dependent permit. Unfortunately, that's prone to starving independent
				 * writes under heavy load, because a dependent write can come in and take all the permits
				 * in between the failure of acquiring the independent write, and the attempt to acquire a dependent
				 * permit.
				 *
				 * Instead, we use a trick Semaphore elevation. First, we acquire a dependent semaphore. Then we
				 * attempt to acquire an independent semaphore. If we can't acquire a dependent, then we can
				 * still attempt to acquire an independent before giving up. If we acquire an indepdent, then
				 * we release the dependent and return independent. Otherwise, we return the dependent.
				 */
				Status dependentStatus = acquireDependentPermit(writes);

				/*
				 * Now we attempt to acquire an independent permit. If we can acquire it AND the dependentStatus
				 * is not rejected, we release the dependent permit that we acquired and return INDEPENDENT.
				 */
				if(acquireIndependent()){
						//we have an  independent thread. Make sure we have room enough in our independent row count to proceed.
						if(!cappedIncrement(independentRowCount,maxIndependentWriteCount,writes)){
								//we do not have enough to acquire an independent. release the independent and return the dependent
								independentValve.release();
								return dependentStatus;
						}else{
								//we acquired the indepdent. Release the dependent permits and return
								if(dependentStatus==Status.DEPENDENT)
										releaseDependentPermit(writes);
								return Status.INDEPENDENT;
						}
				}else return dependentStatus; //we couldn't acquire an independent permit, so use the dependent one
		}

		public boolean releaseIndependentPermit(int writes) {
				independentRowCount.addAndGet(-writes);
				independentValve.release();
				return true;
		}

		public int getDependentRowPermitCount(){
				return dependentRowCount.get();
		}

		public int getIndependentRowPermitCount(){
				return independentRowCount.get();
		}

		public int getDependentThreadCount(){
				return maxDependentWriteThreads-dependentValve.availablePermits();
		}
		public int getIndependentThreadCount(){
				return maxIndependentWriteThreads-independentValve.availablePermits();
		}

		/*****************************************************************************************************************/
		/*private helper methods*/
		private boolean acquireDependent() {
				try {
						return dependentValve.tryAcquire(10l, TimeUnit.MICROSECONDS);
				} catch (InterruptedException e) {
						//we were interrupted, so we can't acquire. Reject
						return true;
				}
		}

		private boolean acquireIndependent() {
				try {
						return independentValve.tryAcquire(10l, TimeUnit.MICROSECONDS);
				} catch (InterruptedException e) {
						//we were interrupted, so we can't acquire. Reject
						return true;
				}
		}

		private boolean cappedIncrement(AtomicInteger value,long maxValue, int writes) {
				boolean shouldContinue;
				do{
						int currCount = value.get();
						int newSum = currCount+writes;
						if(newSum>maxValue){
								return false;
						}

						shouldContinue = !value.compareAndSet(currCount,newSum);

				}while(shouldContinue);
				return true;
		}
}
