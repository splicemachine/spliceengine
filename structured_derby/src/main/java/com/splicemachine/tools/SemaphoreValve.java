package com.splicemachine.tools;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.mortbay.log.Log;

import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Entity for managing throughput of individual actions.
 *
 * A Valve can allow actions to proceed, up to a given maximum value (i.e. the valve is <i>fully open</i>). As
 * actions complete, new actions are allowed to take their place. If more actions are submitted than are allowed
 * through by the valve's current open level, a {@code OpeningPolicy} is consulted as to whether or not the valve
 * should be allowed to open further. If the {@code OpeningPolicy} allows an increase, the valve allows through
 * up to the new maximum. If, however, the {@code OpeningPolicy} does not allow an increase, then the valve will
 * not allow access. It is then up to the caller how to respond to that.
 *
 * @author Scott Fines
 * Created on: 9/6/13
 */
public class SemaphoreValve implements Valve {
    private final OpeningPolicy openingPolicy;
	private static Logger LOG = Logger.getLogger(SemaphoreValve.class);
    private AtomicInteger version;
    private ReducingSemaphore gate;
    private AtomicInteger maxPermits;

    public SemaphoreValve(OpeningPolicy openingPolicy) {
        this.openingPolicy = openingPolicy;
        this.maxPermits = new AtomicInteger(openingPolicy.allowMore(0, SizeSuggestion.DOUBLE));
        this.gate = new ReducingSemaphore(maxPermits.get());
        this.version = new AtomicInteger(0);
    }


		@Override
		public int tryAllow(){
        int i = tryAcquire();
        if(i>=0)
            return i;

        /*
         * We have run out of available permits at the current level.
         * The OpeningPolicy may allow us to expand. If so, we will increase
         * the number of available permits by the difference, and then acquire
         * one.
         */
        //see if the openingpolicy will allow us to expand the semaphore
        attemptIncrease(SizeSuggestion.INCREMENT);
        return tryAcquire();
    }

    private int tryAcquire() {
        if(gate.tryAcquire()){
            return version.get();
        }else return -1;
    }

    private boolean attemptIncrease(SizeSuggestion suggestion) {
        int currentMax = maxPermits.get();
        int nextMax = openingPolicy.allowMore(currentMax,suggestion);
        if(nextMax<=currentMax)
            return true;
        else{
            if( maxPermits.compareAndSet(currentMax,nextMax)){
                gate.release(nextMax - currentMax);
                return true;
            }else{
                return false;
            }
        }
    }

    @Override
		public void release(){
        if(gate.availablePermits()>=maxPermits.get())
            return;

        gate.release();
    }

		@Override
		public void adjustValve(SizeSuggestion suggestion) {
				switch (suggestion) {
						case INCREMENT:
						case DOUBLE:
								adjustUpwards(version.get(),suggestion);
								break;
						case DECREMENT:
						case HALVE:
								reduceValve(version.get(),suggestion);
								break;
				}
		}

		public void reduceValve(int version, SizeSuggestion suggestion){
        if(!this.version.compareAndSet(version,version+1)) return; //someone has already modified the system with this version

        int max = maxPermits.get();
        int newMax = openingPolicy.reduceSize(max,suggestion);
        if (LOG.isDebugEnabled())
        	SpliceLogUtils.debug(LOG, "[%s], reducing valve,currentMax=%d,newMax=%d%n",Thread.currentThread().getName(),max,newMax);
        if(newMax>=max) return; //nothing to do

        if(maxPermits.compareAndSet(max,newMax))
            gate.reducePermits(max-newMax);
        if (LOG.isDebugEnabled())
        	SpliceLogUtils.debug(LOG, "[%s], availablePermits=%d%n",Thread.currentThread().getName(),gate.availablePermits());
    }

		int adjustUpwards(int version, SizeSuggestion suggestion){
        if(this.version.compareAndSet(version,version+1))
            attemptIncrease(suggestion);

        return maxPermits.get();
    }

    @Override
		public int getAvailable() {
        return maxPermits.get();
    }

    public void setMaxPermits(int newMax) {
        int version = this.version.get();
        if(!this.version.compareAndSet(version,version+1))
            return;

        int oldMax = maxPermits.get();
        if(maxPermits.compareAndSet(oldMax,newMax)){
            if(oldMax<newMax){
                gate.release(newMax-oldMax);
            }else if(oldMax > newMax){
                gate.reducePermits(oldMax-newMax);
            }
        }
    }

    public static interface OpeningPolicy{
        /**
         * @param currentSize the current size of the Valve
         * @return the new size of the valve. If the returned value is greater than the
         * current size, then no reduction should occur. If the returned value is negative,
         * the assumed size is zero (fully closed valve)
         */
        public int reduceSize(int currentSize,SizeSuggestion suggestion);

        /**
         * @param currentSize the current size of the valve
         * @return the new size of the valve. If the returned value is <= current size, then
         * no increase is allowed, and overflow actions should be rejected.
         */
        public int allowMore(int currentSize,SizeSuggestion suggestion);
    }

    /**
     * OpeningPolicy which fully opens the valve, and does not close it.
     */
    public static class FixedMaxOpeningPolicy implements OpeningPolicy{
        private final int absoluteMax;

        public FixedMaxOpeningPolicy(int absoluteMax) { this.absoluteMax = absoluteMax; }
        @Override public int reduceSize(int currentSize, SizeSuggestion suggestion) { return absoluteMax; }
        @Override public int allowMore(int currentSize, SizeSuggestion suggestion) { return absoluteMax; }
    }

    public static class PassiveOpeningPolicy implements OpeningPolicy{
        private final int absoluteMax;
        private final AtomicInteger size;

        public PassiveOpeningPolicy(int absoluteMax) {
            this.absoluteMax = absoluteMax;
            this.size = new AtomicInteger(absoluteMax);
        }

        @Override
        public int reduceSize(int currentSize, SizeSuggestion suggestion) {
            int currSize = size.get();
            int newSize;
            switch (suggestion) {
                case DECREMENT:
                    newSize = currSize-1;
                    break;
                case HALVE:
                    newSize = currSize/2;
                    break;
                default:
                    return size.get();
            }
            if(size.compareAndSet(currSize,newSize))
                return newSize;
            return currSize;
        }

        @Override
        public int allowMore(int currentSize, SizeSuggestion suggestion) {
            int currSize = size.get();
            int newSize;
            switch (suggestion) {
                case INCREMENT:
                    newSize = currSize+1;
                    break;
                case DOUBLE:
                    newSize = currSize*2;
                    break;
                default:
                    return size.get();
            }
            if(newSize>absoluteMax)
                newSize=absoluteMax;

            if(currSize<newSize&&size.compareAndSet(currSize,newSize))
                return newSize;
            else
                return currSize;
        }
    }

		private class ReducingSemaphore extends Semaphore{
        public ReducingSemaphore(int permits) { super(permits); }

        @Override public void reducePermits(int reduction) { super.reducePermits(reduction); }
    }
}
