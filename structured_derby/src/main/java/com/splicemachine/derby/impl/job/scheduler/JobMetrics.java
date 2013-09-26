package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.job.Status;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Created on: 9/18/13
 */
class JobMetrics implements JobSchedulerManagement{
    final AtomicLong totalSubmittedJobs = new AtomicLong(0l);
    final AtomicLong totalCompletedJobs = new AtomicLong(0l);
    final AtomicLong totalFailedJobs = new AtomicLong(0l);
    final AtomicLong totalCancelledJobs = new AtomicLong(0l);
    final AtomicInteger numRunningJobs = new AtomicInteger(0);

    void jobFinished(Status finalState){
        numRunningJobs.decrementAndGet();
        switch (finalState) {
            case FAILED:
                totalFailedJobs.incrementAndGet();
                break;
            case COMPLETED:
                totalCompletedJobs.incrementAndGet();
                break;
            case CANCELLED:
                totalCancelledJobs.incrementAndGet();
                break;
        }
    }

    @Override
    public long getTotalSubmittedJobs() {
        return totalSubmittedJobs.get();
    }

    @Override
    public long getTotalCompletedJobs() {
        return totalCompletedJobs.get();
    }

    @Override
    public long getTotalFailedJobs() {
        return totalFailedJobs.get();
    }

    @Override
    public long getTotalCancelledJobs() {
        return totalCancelledJobs.get();
    }

    @Override
    public int getNumRunningJobs() {
        return numRunningJobs.get();
    }
}
