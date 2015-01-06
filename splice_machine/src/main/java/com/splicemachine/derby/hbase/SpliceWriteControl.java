package com.splicemachine.derby.hbase;

import com.splicemachine.utils.TrafficControl;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

public class SpliceWriteControl {

    private final TrafficControl independentTraffic;

    public static enum Status {
        DEPENDENT, INDEPENDENT, REJECTED
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