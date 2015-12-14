package com.splicemachine.si.impl.rollforward;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Date: 9/4/14
 */
public class RollForwardStatus implements RollForwardManagement{
    public final AtomicLong numUpdates = new AtomicLong(0l);
    public final AtomicLong rowsToResolve = new AtomicLong(0l);

    @Override public long getTotalUpdates() { return numUpdates.get(); }
    @Override public long getTotalRowsToResolve() { return  rowsToResolve.get(); }

    public void rowResolved(){
        boolean shouldContinue;
        do{
            long curr = rowsToResolve.get();
            if(curr<=0) return; //we didn't record this row, but we DID resolve it
            shouldContinue = !rowsToResolve.compareAndSet(curr,curr-1);
        }while(shouldContinue);
    }

    public void rowWritten(){
        rowsToResolve.incrementAndGet();
        numUpdates.incrementAndGet();
    }
}
