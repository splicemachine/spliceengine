package com.splicemachine.derby.hbase;

import com.splicemachine.utils.TrafficControl;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * WriteControl limits (or controls) the rate of writes per region server.  It restricts writes based on the number of writes that are currently "in flight"
 * and the number of writer threads that are currently in use.  WriteControl is essentially a multi-variable counting semaphore where the counting variables
 * are the number of current writes and the number of current writer threads.  The limiting variables (or buckets) or further subdivided into independent and 
 * dependent writes.  Independent writes being writes to a single table and dependent writes being writes that require multiple tables to written to such as
 * a base table and its indexes.  WriteControl does not actually perform writes.  It just controls whether or not the write is allowed to proceed.
 * It essentially gives out "permits" when the write request fits within the control limits and rejects write requests when they don't.
 */
public class SpliceWriteControl {

    private final TrafficControl independentTraffic;

    public static enum Status {
        DEPENDENT, INDEPENDENT, nul, REJECTED
    }

    private final AtomicReference<WriteStatus> writeStatus = new AtomicReference<>(new WriteStatus(0, 0, 0, 0));
    protected int maxDependentWriteThreads;
    protected int maxIndependentWriteThreads;
    protected int maxDependentWriteCount;
    protected int maxIndependentWriteCount;

    public SpliceWriteControl(int maxDependentWriteThreads,
                              int maxIndependentWriteThreads, int maxDependentWriteCount, int maxIndependentWriteCount) {
        assert (maxDependentWriteThreads >= 0 &&
                maxIndependentWriteThreads >= 0 &&
                maxDependentWriteCount >= 0 &&
                maxIndependentWriteCount >= 0);
        this.maxIndependentWriteThreads = maxIndependentWriteThreads;
        this.maxDependentWriteThreads = maxDependentWriteThreads;
        this.maxDependentWriteCount = maxDependentWriteCount;
        this.maxIndependentWriteCount = maxIndependentWriteCount;
        this.independentTraffic = new IndepTraffic();
    }

    public Status performDependentWrite(int writes) {
        while (true) {
            WriteStatus state = writeStatus.get();
            if (state.dependentWriteThreads > maxDependentWriteThreads || state.dependentWriteCount > maxDependentWriteCount)
                return Status.REJECTED;
            if (writeStatus.compareAndSet(state, WriteStatus.incrementDependentWriteStatus(state, writes)))
                return Status.DEPENDENT;
        }
    }

    public boolean finishDependentWrite(int writes) {
        while (true) {
            WriteStatus state = writeStatus.get();
            if (writeStatus.compareAndSet(state, WriteStatus.decrementDependentWriteStatus(state, writes)))
                return true;
        }
    }

    public Status performIndependentWrite(int writes) {
        while (true) {
            WriteStatus state = writeStatus.get();
            if (state.independentWriteThreads > maxIndependentWriteThreads || state.independentWriteCount > maxIndependentWriteCount)
                return performDependentWrite(writes); // Attempt to steal
            if (writeStatus.compareAndSet(state, WriteStatus.incrementIndependentWriteStatus(state, writes)))
                return Status.INDEPENDENT;
        }
    }

    public boolean finishIndependentWrite(int writes) {
        while (true) {
            WriteStatus state = writeStatus.get();
            if (writeStatus.compareAndSet(state, WriteStatus.decrementIndependentWriteStatus(state, writes)))
                return true;
        }
    }

    public AtomicReference<WriteStatus> getWriteStatus() {
        return writeStatus;
    }

    public TrafficControl independentTrafficControl() {
        return independentTraffic;
    }

    private class IndepTraffic implements TrafficControl {
        private final ThreadLocal<SpliceWriteControl.Status> statusThread = new ThreadLocal<>();

        @Override
        public void release(int permits) {
            SpliceWriteControl.Status status = statusThread.get();
            if (status == null) return; //null status means rejected, so shouldn't happen anyway
            try {
                switch (status) {
                    case DEPENDENT:
                        finishDependentWrite(permits);
                        break;
                    case INDEPENDENT:
                        finishIndependentWrite(permits);
                        break;
                }
            } finally {
                statusThread.remove();
            }
        }

        @Override
        public int tryAcquire(int minPermits, int maxPermits) {
            SpliceWriteControl.Status status = performIndependentWrite(maxPermits);
            if (status == SpliceWriteControl.Status.REJECTED) return 0;
            else {
                statusThread.set(status);
                return maxPermits;
            }
        }

        @Override
        public void acquire(int permits) throws InterruptedException {
            SpliceWriteControl.Status status = performIndependentWrite(permits);
            while (status == SpliceWriteControl.Status.REJECTED) {
                //park for a random time between 1 and 20 nanoseconds
                LockSupport.parkNanos((long) (Math.random() * 20));
                status = performIndependentWrite(permits);
            }
            statusThread.set(status);
        }

        @Override
        public int getAvailablePermits() {
            return maxIndependentWriteCount - getWriteStatus().get().getIndependentWriteCount();
        }

        @Override
        public int getMaxPermits() {
            return maxIndependentWriteCount;
        }

        @Override
        public void setMaxPermits(int newMaxPermits) {
            throw new UnsupportedOperationException();
        }
    }
}
