package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Abstract RowProvider which assumes a single Scan entity covers the entire data range.
 *
 * @author Scott Fines
 *         Created on: 3/26/13
 */
public abstract class SingleScanRowProvider implements RowProvider {

    protected SpliceRuntimeContext spliceRuntimeContext;
    private static final Logger LOG = Logger.getLogger(SingleScanRowProvider.class);


    @Override
    public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
        return finishShuffle(asyncShuffleRows(instructions),postCompleteTasks);
    }

    @Override
    public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
        return asyncShuffleRows(instructions, toScan());
    }

    private List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions, Scan scan) throws StandardException {
        return Collections.singletonList(
                RowProviders.submitScanJob(scan,
                        SpliceAccessManager.getHTable(getTableName()),
                        instructions,
                        spliceRuntimeContext));
    }

    @Override
    public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobs, Callable<Void>... intermediateCleanupTasks) throws StandardException {
        return RowProviders.completeAllJobs(jobs,false,intermediateCleanupTasks);
    }

    @Override
    public SpliceRuntimeContext getSpliceRuntimeContext() {
        return spliceRuntimeContext;
    }

    /**
     * @return a scan representation of the row provider, or {@code null} if the operation
     * is to be shuffled locally.
     */
    public abstract Scan toScan();

    @Override
    public void close() throws StandardException {

    }

}

