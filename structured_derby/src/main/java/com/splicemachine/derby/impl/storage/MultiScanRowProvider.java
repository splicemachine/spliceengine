package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Abstract RowProvider which assumes that multiple scans are required to
 * cover the entire row space.
 *
 * @author Scott Fines
 *         Created on: 3/26/13
 */
public abstract class MultiScanRowProvider implements RowProvider {
    private static final Logger LOG = Logger.getLogger(MultiScanRowProvider.class);
    protected SpliceRuntimeContext spliceRuntimeContext;

    @Override
    public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
        return finishShuffle(asyncShuffleRows(instructions));
    }

    @Override
    public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
        StandardException baseError = null;
        List<Scan> scans = getScans();
        HTableInterface table = SpliceAccessManager.getHTable(getTableName());
        LinkedList<Pair<JobFuture, JobInfo>> outstandingJobs = Lists.newLinkedList();
        try {
            for (Scan scan : scans) {
                outstandingJobs.add(RowProviders.submitScanJob(scan, table, instructions, spliceRuntimeContext));
            }
            return outstandingJobs;

        } catch (Exception e) {
            baseError = Exceptions.parseException(e.getCause());
            throw baseError;
        } finally {
            if (baseError != null) {
                cancelAll(outstandingJobs);
            }
        }
    }

    @Override
    public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobs) throws StandardException {
        return RowProviders.completeAllJobs(jobs, true);
    }

    private void cancelAll(Collection<Pair<JobFuture, JobInfo>> jobs) {
        //cancel all remaining tasks
        for (Pair<JobFuture, JobInfo> jobToCancel : jobs) {
            try {
                jobToCancel.getFirst().cancel();
            } catch (ExecutionException e) {
                SpliceLogUtils.error(LOG, "Unable to cancel job", e.getCause());
            }
        }
    }

    /**
     * Get all disjoint scans which cover the row space.
     *
     * @return all scans which cover the row space
     * @throws StandardException if something goes wrong while getting scans.
     */
    public abstract List<Scan> getScans() throws StandardException;

    @Override
    public void close() {
    }

}
