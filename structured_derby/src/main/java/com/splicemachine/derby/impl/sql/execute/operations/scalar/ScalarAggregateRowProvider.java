package com.splicemachine.derby.impl.sql.execute.operations.scalar;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.stats.IOStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

/**
 * When no row is returned from the actual operation, this provides a default value ONCE.
 *
 * @author Scott Fines
 *         Created on: 5/21/13
 */
public class ScalarAggregateRowProvider implements RowProvider {

	private boolean defaultReturned;
	private final RowProvider delegate;
	private ExecRow templateRow;
	private boolean populated;

	public ScalarAggregateRowProvider(ExecRow templateRow,
									  SpliceGenericAggregator[] aggregates,
									  RowProvider delegate,
									  boolean returnDefault) throws StandardException {
		this.delegate = delegate;
		this.defaultReturned = !returnDefault;

		initializeTemplateRow(templateRow);

		for (SpliceGenericAggregator aggregate : aggregates) {
			aggregate.getAggregatorInstance(); // necessary?
		}
	}

	/* Set the default values to 0 in case a ProjectRestrictOperation has set the default values to 1.
	   That is done to avoid division by zero exceptions when executing a projection for defining the rows
	   before execution. */
	private void initializeTemplateRow(ExecRow templateRow) throws StandardException {
		this.templateRow = templateRow.getClone();
		this.templateRow.resetRowArray();
		SpliceUtils.populateDefaultValues(this.templateRow.getRowArray(), 0);
	}

	@Override
	public ExecRow next() throws StandardException, IOException {
		if (populated && defaultReturned) {
			populated = false;
			return templateRow;
		}
		return hasNext() ? delegate.next() : null;
	}

	@Override
	public boolean hasNext() throws StandardException, IOException {
		if (delegate.hasNext()) {
			defaultReturned = true;
			return true;
		} else if (!defaultReturned) {
			defaultReturned = true;
			populated = true;
			return true;
		}
		return false;
	}

	/* - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - delegate methods */

	@Override
	public void open() throws StandardException {
		delegate.open();
	}

	@Override
	public void close() throws StandardException {
		delegate.close();
	}

	@Override
	public RowLocation getCurrentRowLocation() {
		return delegate.getCurrentRowLocation();
	}

	@Override
	public byte[] getTableName() {
		return delegate.getTableName();
	}

	@Override
	public int getModifiedRowCount() {
		return delegate.getModifiedRowCount();
	}

	@Override
	public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
		return delegate.shuffleRows(instructions);
	}

	@Override
	public List<Pair<JobFuture, JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
		return delegate.asyncShuffleRows(instructions);
	}

	@Override
	public JobResults finishShuffle(List<Pair<JobFuture, JobInfo>> jobFuture) throws StandardException {
		return delegate.finishShuffle(jobFuture);
	}

	@Override
	public SpliceRuntimeContext getSpliceRuntimeContext() {
		return delegate.getSpliceRuntimeContext();
	}

	@Override
	public void reportStats(long statementId, long operationId, long taskId, String xplainSchema, String regionName) {
		delegate.reportStats(statementId, operationId, taskId, xplainSchema, regionName);
	}

	@Override
	public IOStats getIOStats() {
		return delegate.getIOStats();
	}

}
