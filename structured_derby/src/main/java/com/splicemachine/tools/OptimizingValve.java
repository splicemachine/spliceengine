package com.splicemachine.tools;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Valve that attempts to rate-limit based on minimizing a provided metric.
 *
 * The idea here is that the specified metric is an accurate depiction of load.
 * That is,high value denotes a system which has become saturated,
 * while a low value denotes a system that is underutilized and can
 * bear more load. The goal of this valve then is to allow the maximum
 * number of permits while keeping the load as near to optimal as can be determined.
 *
 * So what is optimal? Here several assumptions can come through, so this
 * implementation allows a {@code Threshold} to be chosen. These strategies accept
 * a mean, variance, and measurement as an option.
 *
 * @author Scott Fines
 * Date: 11/25/13
 */
public class OptimizingValve implements Valve{
		public static enum Position{
				/**
				 * Indicates that the measure is considered too low, and the valve
				 * can increase
				 */
				LOW{
						@Override
						int modifyValve(int currentSize) {
								return currentSize+1;
						}
				},
				/**
				 * Indicates that the measure is too high, and the valve should decrease
				 */
				HIGH{
						@Override
						int modifyValve(int currentSize) {
								return currentSize-1;
						}
				},
				/**
				 * Indicates that the measure is within acceptable range, and the
				 * valve can remain where it is.
				 */
				OK;

				int modifyValve(int currentSize){
						return currentSize;
				}
		}
		public static interface Threshold{
				/**
				 * Determine whether or not the next measurement exceeds the threshold,
				 * and what the Valve should do about it.
				 *
				 * @param measure the next measurement taken
				 * @return the position indicating the movement of the valve (if necessary)
				 */
				Position exceeds(double measure);

				void reset();
		}

		private final AtomicInteger size;
		private final AtomicInteger version = new AtomicInteger(0);
		private final ReducingSemaphore semaphore;
		private final Threshold latencyThreshold;
		private final Threshold throughputThreshold;
		private final int maxPermits;

		public OptimizingValve(int initialPermits,
													 int maxPermits,
													 Threshold latencyThreshold,
													 Threshold throughputThreshold) {
				this.size = new AtomicInteger(initialPermits);
				this.semaphore = new ReducingSemaphore(initialPermits);
				this.throughputThreshold = throughputThreshold;
				this.latencyThreshold = latencyThreshold;
				this.maxPermits= maxPermits;
		}

		public void update(double latency, double throughput){
				Position latencyPosition=latencyThreshold.exceeds(latency);
				Position throughputPosition=throughputThreshold.exceeds(throughput);
				Position position=Position.OK;
				if(latencyPosition==Position.HIGH){
						//if the latency is high, we need to bring it down
						position = latencyPosition;
				}else if(throughputPosition==Position.LOW)
						position = throughputPosition;

				switch (position) {
						case OK:
								return; //nothing to do
						case LOW:
								open(SizeSuggestion.INCREMENT);
								break;
						case HIGH:
								close(SizeSuggestion.DECREMENT);
								break;
				}
		}

		@Override
		public void adjustValve(SizeSuggestion suggestion) {
				int currSize = size.get();
				switch (suggestion) {
						case INCREMENT:
						case DOUBLE:
								if (currSize >= maxPermits) return; //can't open any further;
								open(suggestion);
								break;
						case DECREMENT:
						case HALVE:
								if(currSize<=0) return;
								close(suggestion);
								break;
				}
		}

		@Override
		public int tryAllow() {
				if(semaphore.tryAcquire()){
						return version.get();
				}else return -1;
		}

		@Override
		public void release() {
				//allow the permit to remain acquired if we reduced the valve size
				if(semaphore.availablePermits()>=size.get()){
						return;
				}
				semaphore.release();
		}

		@Override
		public int getAvailable() {
				return semaphore.availablePermits();
		}

		@Override
		public void setMaxPermits(int newMax) {
				//no-op
		}

		private void close(SizeSuggestion suggestion) {
//				System.out.printf("[%s] closing with suggestion %s%n",Thread.currentThread(),suggestion);
				int currVersion = version.get();
				//if someone else beats us to the opening and/or closing,
				//don't override them
				if(!this.version.compareAndSet(currVersion,currVersion+1)) return;

				int currSize = size.get();
				int newSize = suggestion.adjust(currSize);
				if(size.compareAndSet(currSize,newSize)){
//						System.out.printf("[%s] currSize=%d, newSize=%d%n",Thread.currentThread(),currSize,newSize);
						semaphore.reducePermits(currSize-newSize);
				}
		}

		private void open(SizeSuggestion suggestion) {
				int currVersion = version.get();
				//if someone else beats us to the opening and/or closing,
				//don't override them
				if(!this.version.compareAndSet(currVersion,currVersion+1)) return;

				int currSize = size.get();
				int newSize = suggestion.adjust(currSize);
				if(size.compareAndSet(currSize,newSize)){
						semaphore.release(newSize-currSize);
				}
		}

		private static class ReducingSemaphore extends Semaphore {
				public ReducingSemaphore(int permits) { super(permits); }

				@Override public void reducePermits(int reduction) { super.reducePermits(reduction); }
		}
}
