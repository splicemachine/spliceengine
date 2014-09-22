package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.IOStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * RowProvider for the export result status message that comes back to the client when export is finished.  Currently
 * we return one row reporting the number of rows exported.
 */
class ExportRowProvider implements RowProvider {

    private final SpliceRuntimeContext spliceRuntimeContext;
    private final long exportedRowCount;
    private final long totalTimeMillis;
    private boolean reportedResult;

    ExportRowProvider(SpliceRuntimeContext spliceRuntimeContext, long exportedRowCount, long totalTimeMillis) {
        this.spliceRuntimeContext = spliceRuntimeContext;
        this.exportedRowCount = exportedRowCount;
        this.totalTimeMillis = totalTimeMillis;
    }

    @Override
    public boolean hasNext() throws StandardException, IOException {
        if (!reportedResult) {
            reportedResult = true;
            return true;
        }
        return false;
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        ExecRow currentTemplate = new ValueRow(2);
        currentTemplate.setRowArray(new DataValueDescriptor[]{
                new SQLLongint(exportedRowCount),
                new SQLLongint(totalTimeMillis)
        });
        return currentTemplate;
    }

    @Override
    public void open() throws StandardException {
    }

    @Override
    public RowLocation getCurrentRowLocation() {
        return null;
    }

    @Override
    public JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException {
        return null;
    }

    @Override
    public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException {
        return null;
    }

    @Override
    public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFuture, Callable<Void>... intermediateCleanupTasks) throws StandardException {
        return null;
    }

    @Override
    public byte[] getTableName() {
        return null;
    }

    @Override
    public int getModifiedRowCount() {
        return 0;
    }

    @Override
    public SpliceRuntimeContext getSpliceRuntimeContext() {
        return spliceRuntimeContext;
    }

    @Override
    public void reportStats(long statementId, long operationId, long taskId, String xplainSchema, String regionName) throws IOException {
    }

    @Override
    public IOStats getIOStats() {
        return null;
    }

    @Override
    public void close() throws StandardException {
    }

}
