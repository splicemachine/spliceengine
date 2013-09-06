package com.splicemachine.tools;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

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
public class Valve {
    private final OpeningPolicy openingPolicy;

    private AtomicInteger version;
    private ReducingSemaphore gate;
    private AtomicInteger maxPermits;

    public Valve(OpeningPolicy openingPolicy) {
        this.openingPolicy = openingPolicy;
        this.maxPermits = new AtomicInteger(openingPolicy.allowMore(0, OpeningPolicy.SizeSuggestion.DOUBLE));
        this.gate = new ReducingSemaphore(maxPermits.get());
        this.version = new AtomicInteger(0);
    }

    public int tryAllow(){
        int i = tryAcquire();
        if(i>=0)
            return i;

        //see if the openingpolicy will allow us to expand the semaphore
        attemptIncrease(version.get());
        return tryAcquire();
//        if(attemptIncrease(version.get()))
//            return tryAcquire();
//        else
//            return -1;
    }

    private int tryAcquire() {
        if(gate.tryAcquire()){
            return version.get();
        }else return -1;
    }

    private boolean attemptIncrease(int version) {
        System.out.printf("[%s] attempting increase,version=%d%n",Thread.currentThread().getName(),version);
        int currentMax = maxPermits.get();
        int nextMax = openingPolicy.allowMore(currentMax, OpeningPolicy.SizeSuggestion.INCREMENT);
        System.out.printf("[%s] currentMax=%d,nextMax=%d%n",Thread.currentThread().getName(),currentMax,nextMax);
        if(nextMax<=currentMax)
            return true;
        else{
            //make sure nobody beat us to the punch with the version
//            if(!this.version.compareAndSet(version,version+1)){
//                System.out.printf("[%s], did not set version%n",Thread.currentThread().getName());
//                return true;
//            }else
            if( maxPermits.compareAndSet(currentMax,nextMax)){
                System.out.printf("[%s], set maxPermits safely%n",Thread.currentThread().getName());
                gate.release(nextMax - currentMax);
                return true;
            }else{
                System.out.printf("[%s], did not set maxPermits%n",Thread.currentThread().getName());
                return true;
            }
        }
    }

    public void release(){
        if(gate.availablePermits()>=maxPermits.get())
            return;

        gate.release();
    }

    public void reduceValve(int version,OpeningPolicy.SizeSuggestion suggestion){
        if(!this.version.compareAndSet(version,version+1)) return; //someone has already modified the system with this version

        int max = maxPermits.get();
        int newMax = openingPolicy.reduceSize(max,suggestion);
        System.out.printf("[%s], reducing valve,currentMax=%d,newMax=%d%n",Thread.currentThread().getName(),max,newMax);
        if(newMax>=max) return; //nothing to do

        if(maxPermits.compareAndSet(max,newMax))
            gate.reducePermits(max-newMax);
        System.out.printf("[%s], availablePermits=%d%n",Thread.currentThread().getName(),gate.availablePermits());
    }

    public int adjustUpwards(int version,OpeningPolicy.SizeSuggestion suggestion){
        if(this.version.compareAndSet(version,version+1))
            attemptIncrease(version);

        return maxPermits.get();
    }

    public static interface OpeningPolicy{
        public static enum SizeSuggestion{
            INCREMENT,
            DECREMENT,
            HALVE,
            DOUBLE;
        }
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

        public FixedMaxOpeningPolicy(int absoluteMax) {
            this.absoluteMax = absoluteMax;
        }

        @Override
        public int reduceSize(int currentSize, SizeSuggestion suggestion) {
            return absoluteMax;
        }

        @Override
        public int allowMore(int currentSize, SizeSuggestion suggestion) {
            return absoluteMax;
        }
    }

    public static class PassiveOpeningPolicy implements OpeningPolicy{
        private final int absoluteMax;

        private AtomicInteger size;
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

    /**
     * OpeningPolicy in which the valve is either fully closed, or fully open.
     */
    public static class BinaryValvePolicy implements OpeningPolicy{
        private final int absoluteMax;

        public BinaryValvePolicy(int absoluteMax) {
            this.absoluteMax = absoluteMax;
        }

        @Override
        public int reduceSize(int currentSize, SizeSuggestion suggestion) {
            return 0;
        }

        @Override
        public int allowMore(int currentSize, SizeSuggestion suggestion) {
            return absoluteMax;
        }
    }

    private class ReducingSemaphore extends Semaphore{

        public ReducingSemaphore(int permits) {
            super(permits);
        }

        private ReducingSemaphore(int permits, boolean fair) {
            super(permits, fair);
        }

        @Override
        public void reducePermits(int reduction) {
            super.reducePermits(reduction);
        }
    }
}
