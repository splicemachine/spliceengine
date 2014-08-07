package com.splicemachine.derby.iapi.storage;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.metrics.IOStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Provides ExecRows for later stages in the execution computation.
 *
 * Originally, HBase Scans were passed around; This means that situations
 * where there are no scans were unable to function (e.g. inserting a single row, or
 *  a small collection of rows). To alleviate this issue, while still providing for
 * a mechanism for when Scans <em>are</em> useable, This interface exists.
 */
public interface RowProvider extends RowProviderIterator<ExecRow>  {

	/**
	 * Calling multiple times should result in a safe re-opening
	 * of the iterator.
	 * @throws StandardException 
	 */
	void open() throws StandardException;
	
	/**
	 * Close the iterator
	 */
	void close() throws StandardException;

    /**
     * @return the current row location.
     */
	RowLocation getCurrentRowLocation();

    /**
     * Execute the constructed instructions against all the rows provided by this row provider.
     *
     *
		 * @param instructions the instructions to execute
		 * @param postCompleteTasks
		 * @throws StandardException if something goes wrong during the shuffle phase
     */
    JobResults shuffleRows(SpliceObserverInstructions instructions, Callable<Void>... postCompleteTasks) throws StandardException, IOException;

    List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException, IOException;

    JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFuture, Callable<Void>... intermediateCleanupTasks) throws StandardException;

    /**
     * Gets the "table name" of the backing storage, or {@code null} if there is none.
     *
     * @return the table name, or {@code null} if no table name exists
     */
    byte[] getTableName();

    /**
     * @return the number of rows which are being modified, or 0 if no rows are being modified by this (e.g.
     * if it's a scan).
     */
    int getModifiedRowCount();

    /**
     * Retrieve the runtime context.
     * @return
     */
    SpliceRuntimeContext getSpliceRuntimeContext();

    void reportStats(long statementId,
                     long operationId,
                     long taskId,
                     String xplainSchema,
                     String regionName);

	IOStats getIOStats();
}
