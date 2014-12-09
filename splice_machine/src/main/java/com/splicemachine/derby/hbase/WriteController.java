package com.splicemachine.derby.hbase;

import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;
import com.splicemachine.utils.TrafficControl;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 11/25/14
 */
public abstract class WriteController implements WriteControl{
    private final int maxThreads;
    private final Semaphore availableThreads;
    private final TrafficControl dependentWrites;
    private final TrafficControl independentWrites;

    public WriteController(int maxThreads, int maxIndependentWrites, int maxDependentWrites) {
        this.availableThreads = new Semaphore(maxThreads);
        this.dependentWrites = newDependentController(availableThreads,maxDependentWrites);
        this.independentWrites = newIndependentController(availableThreads,dependentWrites,maxIndependentWrites);
        this.maxThreads = maxThreads;
    }

    protected abstract TrafficControl newIndependentController(Semaphore availableThreads,TrafficControl dependentWrites,int maxIndependentWrites);

    protected abstract TrafficControl newDependentController(Semaphore availableThreads,int maxDependentWrites);

    public static WriteControl concurrencyWriteControl(int maxThreads, int maxIndependentWrites, int maxDependentWrites){
        return new ConcurrencyWriteController(maxThreads,maxIndependentWrites,maxDependentWrites);
    }

    @SuppressWarnings("UnusedDeclaration")
    public static WriteControl throughputWriteControl(int maxThreads, int maxIndepThroughput, int maxDependentThroughput){
        return new WriteController(maxThreads,maxIndepThroughput,maxDependentThroughput) {
            @Override
            protected TrafficControl newIndependentController(Semaphore availableThreads,
                                                              TrafficControl dependentWrites,
                                                              int maxIndependentWrites) {
                return new IndependentThroughputTrafficControl(availableThreads,dependentWrites,maxIndependentWrites);
            }

            @Override
            protected TrafficControl newDependentController(Semaphore availableThreads,
                                                            int maxDependentWrites) {
                return new DependentThroughputTrafficControl(availableThreads,maxDependentWrites);
            }
        };
    }

    @Override
    public TrafficControl dependentControl(){
        return dependentWrites;
    }

    @Override
    public TrafficControl independentControl(){
        return independentWrites;
    }

    @Override
    public int getOccupiedThreads(){
       return maxThreads-availableThreads.availablePermits();
    }

    @Override
    public int maxWriteThreads(){
        return maxThreads;
    }

    @Override
    public int getAvailableDependentPermits(){
        return dependentWrites.getAvailablePermits();
    }

    @Override
    public int getAvailableIndependentPermits(){
        return independentWrites.getAvailablePermits();
    }

    @Override
    public int getMaxDependentPermits(){
        return dependentWrites.getMaxPermits();
    }

    @Override
    public int getMaxIndependentPermits(){
        return independentWrites.getMaxPermits();
    }

    @Override
    public void setMaxIndependentPermits(int newMaxIndependentThroughput){
        independentWrites.setMaxPermits(newMaxIndependentThroughput);
    }

    public void setMaxDependentPermits(int newMaxDependentThroughput){
        dependentWrites.setMaxPermits(newMaxDependentThroughput);
    }

    private static boolean acquireWriteThread(Semaphore semaphore) {
        try {
            return semaphore.tryAcquire(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            /*
             * we couldn't acquire a write thread, so reject this entirely
             */
            Thread.currentThread().interrupt(); //make sure the thread's interrupt flag is set
            return false;
        }
    }

    private static class ConcurrencyWriteController extends WriteController{

        public ConcurrencyWriteController(int maxThreads, int maxIndependentWrites, int maxDependentWrites) {
            super(maxThreads, maxIndependentWrites, maxDependentWrites);
        }

        @Override
        protected TrafficControl newIndependentController(Semaphore availableThreads,TrafficControl dependentWrites,int maxIndependentWrites) {
            return new IndependentConcurrencyTrafficControl(availableThreads,dependentWrites,maxIndependentWrites);
        }

        @Override
        protected TrafficControl newDependentController(Semaphore availableThreads,int maxDependentWrites) {
            return new DependentConcurrencyTrafficControl(availableThreads,maxDependentWrites);
        }
    }
    private static class IndependentConcurrencyTrafficControl extends ConcurrencyTrafficControl {
        private final TrafficControl dependentWrites;

        public IndependentConcurrencyTrafficControl(Semaphore availableThreads,
                                                    TrafficControl dependentWrites,int maxWrites) {
            super(availableThreads, maxWrites);
            this.dependentWrites = dependentWrites;
        }

        @Override
        public void release(int permits) {
            if(permits<0) return; //we didn't acquire the write thread
            int overflow = releaseAmount(permits);
            if(overflow>0)
                dependentWrites.release(overflow);
            availableThreads.release();
        }

        @Override
        public int tryAcquire(int minPermits, int maxPermits) {
            if(!acquireWriteThread(availableThreads)) return -1;
            int acquired = dependentWrites.tryAcquire(minPermits, maxPermits);
            if(acquired==maxPermits) return acquired;

            minPermits = 0;
            maxPermits-=acquired;
            return acquired+dependentWrites.tryAcquire(minPermits,maxPermits);
        }
    }

    private static class DependentConcurrencyTrafficControl extends ConcurrencyTrafficControl{

        public DependentConcurrencyTrafficControl(Semaphore availableThreads, int maxWrites) {
            super(availableThreads, maxWrites);
        }

        @Override
        public int tryAcquire(int minPermits, int maxPermits) {
            if(!acquireWriteThread(availableThreads)) return -1;
            return super.tryAcquire(minPermits, maxPermits);
        }

        @Override
        public void release(int permitsAcquired) {
            if(permitsAcquired<0) return; //we didn't acquire the write thread
            super.release(permitsAcquired);
            availableThreads.release();
        }
    }

    private static class ConcurrencyTrafficControl implements TrafficControl {
        protected Semaphore availableThreads;
        private final AtomicInteger availableWrites;
        private volatile int maxWrites;

        public ConcurrencyTrafficControl(Semaphore availableThreads,int maxWrites) {
            this.availableWrites = new AtomicInteger(maxWrites);
            this.maxWrites = maxWrites;
            this.availableThreads = availableThreads;
        }

        public int tryAcquire(int minPermits, int maxPermits) {
            //stash value into temp variable to avoid doubling up on the volatile read
            int mw = maxWrites;
            if (maxPermits > mw)
                maxPermits = mw;

            int acquired = 0;
            do {
                int currAvailable = availableWrites.get();
                if (currAvailable < minPermits) {
                /*
                 * In this scenario, there aren't enough permits to satisfy even
                 * the minimum permit request, so we have to bail and return 0.
                 */
                    break;
                }
            /*
             * We have enough to fulfill the minimum permit requirement, so we
             * take as many as we can. If we are able to perform the CAS, then we
             * have acquired that many permits. Otherwise, we try again.
             */
                int newTotal = currAvailable;
                int permitsAcquired = 0;
                if (currAvailable >= maxPermits) {
                    //try and grab them all
                    newTotal -= maxPermits;
                    permitsAcquired += maxPermits;
                } else {
                    newTotal -= currAvailable;
                    permitsAcquired += maxPermits;
                }

                //do a compare and swap to see if we successfully acquired that many permits
                boolean success = availableWrites.compareAndSet(currAvailable, newTotal);
                if (success) {
                    acquired += permitsAcquired;
                }
            } while (acquired < minPermits);

            return acquired;
        }

        @Override
        public void acquire(int permits) throws InterruptedException {
            throw new UnsupportedOperationException("IMPLEMENT");
        }

        public int releaseAmount(int permitsAcquired) {
            boolean shouldContinue;
            int overflow;
            do {
                int currAvailable = availableWrites.get();
                int newTotal = currAvailable + permitsAcquired;
            /*
             * new total should never be higher than maxPermits, because we shouldn't
             * ever be able to acquire more than that at one time, but just in case
             * we'll cap it to the max.
             *
             * Note that we can't avoid the volatile read here because someone
             * may have set a new max in the middle of our looping, and we need
             * to obey that contract
             */
                overflow = Math.max(0,newTotal-maxWrites);
                newTotal-=overflow;
                shouldContinue = !availableWrites.compareAndSet(currAvailable, newTotal);
            } while (shouldContinue);

            return overflow;
        }

        public void release(int permitsAcquired) {
            releaseAmount(permitsAcquired);
        }

        @Override
        public int getAvailablePermits() {
            return maxWrites - availableWrites.get();
        }

        @Override
        public int getMaxPermits() {
            return maxWrites;
        }

        @Override
        public void setMaxPermits(int newMaxPermits) {
            int oldMax = maxWrites;
            this.maxWrites = newMaxPermits;
            if (newMaxPermits > oldMax) {
                releaseAmount(newMaxPermits - oldMax); //add in the extra permits
            }
        }
    }

    private static class IndependentThroughputTrafficControl extends ThroughputTrafficControl{
        private final TrafficControl dependentWrites;

        public IndependentThroughputTrafficControl(Semaphore availableThreads,
                                                   TrafficControl dependentWrites,
                                                   int throughput) {
            super(availableThreads, throughput);
            this.dependentWrites = dependentWrites;
        }

        @Override
        public int tryAcquire(int minPermits, int maxPermits) {
            if(!acquireWriteThread(availableThreads)) return -1;
            int acquired = dependentWrites.tryAcquire(minPermits,maxPermits);
            if(acquired==maxPermits) return acquired;

            minPermits=0;
            maxPermits-=acquired;
            return acquired+super.tryAcquire(minPermits, maxPermits);
        }

        @Override
        public void release(int permits) {
            dependentWrites.release(permits);
        }
    }

    private static class DependentThroughputTrafficControl extends ThroughputTrafficControl{


        public DependentThroughputTrafficControl(Semaphore availableThreads, int throughput) {
            super(availableThreads, throughput);
        }

        @Override
        public int tryAcquire(int minPermits, int maxPermits) {
            if(!acquireWriteThread(availableThreads)) return -1;
            return super.tryAcquire(minPermits, maxPermits);
        }
    }

    private static class ThroughputTrafficControl implements TrafficControl{
        private final TrafficController throughputControl;
        protected final Semaphore availableThreads;

        public ThroughputTrafficControl(Semaphore availableThreads,int throughput) {
            this.throughputControl = TrafficShaping.fixedRateTrafficShaper(throughput,throughput,TimeUnit.SECONDS);
            this.availableThreads = availableThreads;
        }

        @Override
        public void release(int permits) {
            if(permits<0) return; //we didn't acquire the write thread
            availableThreads.release();
        }

        @Override
        public void acquire(int permits) throws InterruptedException {
            throw new UnsupportedOperationException("IMPLEMENT");
        }

        @Override
        public int tryAcquire(int minPermits, int maxPermits) {
            return throughputControl.tryAcquire(minPermits,maxPermits);
        }

        @Override
        public int getAvailablePermits() {
            return throughputControl.availablePermits();
        }

        @Override
        public int getMaxPermits() {
            return throughputControl.maxPermits();
        }

        @Override
        public void setMaxPermits(int newMaxPermits) {
            throughputControl.setMaxPermits(newMaxPermits);
        }
    }
}
