package com.splicemachine.derby.hbase;

import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 11/14/14
 */
public class WriteControl {
    private final Semaphore threadCount;
    private final TrafficController totalThroughputControl;
    private final TrafficController dependentThroughputControl;
    private final int maxThreads;

    public WriteControl(int maxThreads,
                        int totalRowsPerSecond,
                        int dependentRowsPerSecond) {
        this.threadCount = new Semaphore(maxThreads);
        this.maxThreads = maxThreads;
        this.totalThroughputControl = TrafficShaping.fixedRateTrafficShaper(totalRowsPerSecond,totalRowsPerSecond, TimeUnit.SECONDS);
        this.dependentThroughputControl = TrafficShaping.fixedRateTrafficShaper(dependentRowsPerSecond,dependentRowsPerSecond,TimeUnit.SECONDS);
    }

    public int acquireIndependentPermits(int minPermits,int maxPermits) {
        //make sure that we can have threaded-access. If not, reject entire write
        try {
            if (!threadCount.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                return -1; //inform the caller that we couldn't even get a thread
            }
            int acquired = dependentThroughputControl.tryAcquire(minPermits, maxPermits);
            /*
             * Acquired will be one of the following states:
             *
             * 1. 0 => No available independent writes
             * 2. minPermits<=acquired<maxPermits => try and get the rest from the dependent
             * 3. maxPermits => we are done
             */
            if(acquired==maxPermits)
                return acquired;
            else if(acquired>minPermits) {
                minPermits = 0;
                maxPermits = maxPermits-acquired;
            }
            return totalThroughputControl.tryAcquire(minPermits,maxPermits);
        }catch(InterruptedException ie){
            return 0; //interrupted means reject outright
        }
    }

    public int acquireDependentPermits(int minPermits,int maxPermits)  {
        //make sure that we can have threaded-access. If not, reject entire write
        try {
            if (!threadCount.tryAcquire(10, TimeUnit.MILLISECONDS)) {
                return -1; //we couldn't even get a thread
            }
            return dependentThroughputControl.tryAcquire(minPermits, maxPermits);
        }catch(InterruptedException ie){
            return 0; //interrupted means reject outright
        }
    }

    public void releasePermits(int permitsAcquired){
        if(permitsAcquired>=0)
            threadCount.release();
    }

    public int getOccupiedThreads() {
        return maxThreads-threadCount.availablePermits();
    }

    public int maxWriteThreads() {
        return maxThreads;
    }

    public int getAvailableDependentPermits() {
        return dependentThroughputControl.availablePermits();
    }

    public int getAvailableIndependentPermits() {
        return totalThroughputControl.availablePermits();
    }

    public int getMaxDependentPermits() {
        return dependentThroughputControl.maxPermits();
    }

    public int getMaxIndependentPermits() {
        return totalThroughputControl.maxPermits();
    }

    public void setMaxIndependentPermits(int newMaxIndependenThroughput) {
        totalThroughputControl.setMaxPermits(newMaxIndependenThroughput);
    }

    public void setMaxDependentPermits(int newMaxDependentThroughput) {
        dependentThroughputControl.setMaxPermits(newMaxDependentThroughput);
    }
}
