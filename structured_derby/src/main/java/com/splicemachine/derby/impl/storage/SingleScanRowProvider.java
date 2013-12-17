package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.SimpleJobResults;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract RowProvider which assumes a single Scan entity covers the entire data range.
 *
 * @author Scott Fines
 * Created on: 3/26/13
 */
public abstract class SingleScanRowProvider  implements RowProvider {

	protected SpliceRuntimeContext spliceRuntimeContext;
    private static final Logger LOG = Logger.getLogger(SingleScanRowProvider.class);
    private boolean shuffled = false;
		private JobFuture jobFuture;

    @Override
    public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
    	shuffled=true;
        Scan scan = toScan();
        instructions.setSpliceRuntimeContext(spliceRuntimeContext);
				spliceRuntimeContext.setCurrentTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
        if(scan==null){
            //operate locally
            SpliceOperation op = instructions.getTopOperation();
            op.init(SpliceOperationContext.newContext(op.getActivation()));
            try{
                OperationSink opSink = OperationSink.create((SinkingOperation) op,null,instructions.getTransactionId());

								JobStats stats;
                if(op instanceof DMLWriteOperation)
                    stats =new LocalTaskJobStats(opSink.sink(((DMLWriteOperation)op).getDestinationTable(), spliceRuntimeContext));
                else{
										byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
                    stats =new LocalTaskJobStats(opSink.sink(tempTableBytes, spliceRuntimeContext));
								}

								return new SimpleJobResults(stats,null);
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }else{
            HTableInterface table = SpliceAccessManager.getHTable(getTableName());
            try{
                return doShuffle(table, instructions, scan);
            }finally{
                try {
                    table.close();
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
            }
        }
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

				if(jobFuture!=null){
						try{
								jobFuture.cleanup();
						}catch(ExecutionException e){
								throw Exceptions.parseException(e);
						}
				}
    }

    /********************************************************************************************************************/
    /*private helper methods*/

    private JobResults doShuffle(HTableInterface table,
                           SpliceObserverInstructions instructions, Scan scan) throws StandardException {
        SpliceUtils.setInstructions(scan,instructions);
        
        //determine if top operation writes data
        boolean readOnly = !(instructions.getTopOperation() instanceof DMLWriteOperation);
        OperationJob job = new OperationJob(scan,instructions,table,readOnly);
        StandardException baseError = null;
        try {
            jobFuture = SpliceDriver.driver().getJobScheduler().submit(job);
            //wait for everyone to complete, or somebody to error out
            jobFuture.completeAll();

            return new SimpleJobResults(jobFuture.getJobStats(),jobFuture);
        } catch (ExecutionException ee) {
        	SpliceLogUtils.error(LOG, ee);
            baseError = Exceptions.parseException(ee.getCause());
            throw baseError;
        } catch (InterruptedException e) {
        	SpliceLogUtils.error(LOG, e);
        	baseError = Exceptions.parseException(e);
            throw baseError;
        }finally{
            if(baseError!=null){
                SpliceLogUtils.logAndThrow(LOG,baseError);
            }
        }
    }

    private class LocalTaskJobStats implements JobStats {
        private final TaskStats stats;

        public LocalTaskJobStats(TaskStats stats) {
            this.stats = stats;
        }

        @Override public int getNumTasks() { return 1; }
        @Override public int getNumSubmittedTasks() { return 1; }
        @Override public int getNumCompletedTasks() { return 1; }
        @Override public int getNumFailedTasks() { return 0; }
        @Override public int getNumInvalidatedTasks() { return 0; }
        @Override public int getNumCancelledTasks() { return 0; }
        @Override public long getTotalTime() { return stats.getTotalTime(); }
        @Override public String getJobName() { return "localJob"; }

        @Override
        public List<byte[]> getFailedTasks() {
            return Collections.emptyList();
        }

        @Override
        public List<TaskStats> getTaskStats() {
            return Arrays.asList(stats);
        }
    }
}
