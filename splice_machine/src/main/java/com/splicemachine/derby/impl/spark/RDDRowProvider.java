package com.splicemachine.derby.impl.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.sql.execute.operations.EmptyJobStats;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.SimpleJobResults;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.spark.api.java.JavaRDD;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.SpliceUtils;

public class RDDRowProvider implements RowProvider, Serializable {

    private SpliceRuntimeContext spliceRuntimeContext;

    // TODO missing activation for shuffle of rows for operations higher up
    public RDDRowProvider(JavaRDD<LocatedRow> javaRDD, SpliceRuntimeContext spliceRuntimeContext) {
        this.rdd = javaRDD;
        this.spliceRuntimeContext = spliceRuntimeContext;
    }

    private static final long serialVersionUID = -6767694441802309601L;
    protected transient Iterator<LocatedRow> iterator;
    protected ExecRow currentRow;
    private HBaseRowLocation currentRowLocation;
    protected JavaRDD<LocatedRow> rdd;

    @Override
    public boolean hasNext() throws StandardException, IOException {
        if (iterator.hasNext()) {
            this.currentRow = iterator.next().getRow();
            this.currentRowLocation = new HBaseRowLocation(SpliceUtils.getUniqueKey());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ExecRow next() throws StandardException {
        return this.currentRow;
    }

    @Override
    public void open() throws StandardException {
        iterator = this.rdd.collect().iterator();
    }

    @Override
    public void close() {
        // no op
    }

    @Override
    public RowLocation getCurrentRowLocation() {
        return currentRowLocation;
    }

    @Override
    public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException {
        return new SimpleJobResults(new EmptyJobStats(), null);
    }

    @Override
    public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
        return Collections.emptyList();
    }

    @Override
    public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFuture, Callable<Void>... intermediateCleanupTasks) throws StandardException {
        return new SimpleJobResults(new EmptyJobStats(), null);
    }

    @Override
    public byte[] getTableName() {
        return SpliceConstants.TEMP_TABLE_BYTES;
    }

    @Override
    public int getModifiedRowCount() {
        return 0;
    }

    public JavaRDD<LocatedRow> getRDD() {
        return rdd;
    }

    @Override
    public SpliceRuntimeContext getSpliceRuntimeContext() {
        return spliceRuntimeContext;
    }

    @Override
    public void reportStats(long statementId, long operationId, long taskId, String xplainSchema, String regionName) {

    }

    @Override
    public IOStats getIOStats() {
        return Metrics.noOpIOStats();
    }
}