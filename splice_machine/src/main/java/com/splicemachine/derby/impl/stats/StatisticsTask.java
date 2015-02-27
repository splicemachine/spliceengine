package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;

import java.util.concurrent.ExecutionException;

/**
 * Asynchronous task to collect statistics for a region.
 *
 * @author Scott Fines
 *         Date: 2/26/15
 */
public class StatisticsTask extends ZkTask{
    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {

    }

    @Override
    protected String getTaskType() {
        return null;
    }

    @Override
    public boolean invalidateOnClose() {
        return false;
    }

    @Override
    public RegionTask getClone() {
        return null;
    }

    @Override
    public int getPriority() {
        return 0;
    }
}
