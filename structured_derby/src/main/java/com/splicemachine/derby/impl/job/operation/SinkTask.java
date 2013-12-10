package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class SinkTask extends ZkTask {
    private static final long serialVersionUID = 3l;
    private static final Logger LOG = Logger.getLogger(SinkTask.class);
    private HRegion region;

    private Scan scan;
    private SpliceObserverInstructions instructions;

		/*
		 * Hash bucket to use for sink operations which do not spread data themselves.
		 *
		 * For example, the SortOperation can't spread data around multiple buckets, so
		 * it will use this hashBucket to determine which bucket to go to. The
		 * bucket itself will be generated randomly, to (hopefully) spread data from multiple
		 * concurrent operations across multiple buckets.
		 */
		private byte hashBucket;

    /**
     * Serialization Constructor.
     */
    @SuppressWarnings("UnusedDeclaration")
		public SinkTask(){ super(); }

    public SinkTask(String jobId,
                    Scan scan,
                    String transactionId,
                    boolean readOnly,
                    int priority) {
        super(jobId,priority,transactionId,readOnly);
        this.scan = scan;

				List<byte[]> taskChain = OperationSink.taskChain.get();
				if(taskChain!=null){
						parentTaskId = taskChain.get(taskChain.size()-1);
				}
		}

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce,SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        //make sure that our task id is properly set
        this.region = rce.getRegion();
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"executing task %s",Bytes.toString(getTaskId()));
        SpliceTransactionResourceImpl impl = null;
        boolean prepared=false;
        SpliceOperationContext opContext = null;
        try {
            impl = new SpliceTransactionResourceImpl();
            impl.prepareContextManager();
            prepared=true;
            if(instructions==null)
                instructions = SpliceUtils.getSpliceObserverInstructions(scan);
            impl.marshallTransaction(instructions);
						Activation activation = instructions.getActivation(impl.getLcc());
            SpliceRuntimeContext spliceRuntimeContext = instructions.getSpliceRuntimeContext();
            spliceRuntimeContext.markAsSink();
						spliceRuntimeContext.setCurrentTaskId(getTaskId());
            opContext = new SpliceOperationContext(region,
                    scan,activation,instructions.getStatement(),impl.getLcc(),true,instructions.getTopOperation(), spliceRuntimeContext);
            //init the operation stack

            SpliceOperation op = instructions.getTopOperation();
            op.init(opContext);
            OperationSink opSink = OperationSink.create((SinkingOperation) op, getTaskId(), getTransactionId());

            
            TaskStats stats;
            if(op instanceof DMLWriteOperation)
                stats = opSink.sink(((DMLWriteOperation)op).getDestinationTable(), spliceRuntimeContext);
            else{
								TempTable table = SpliceDriver.driver().getTempTable();
                stats = opSink.sink(table.getTempTableName(), spliceRuntimeContext);
						}
            status.setStats(stats);

						if(LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG,"task %s sunk successfully, closing", Bytes.toString(getTaskId()));
        } catch (Exception e) {
            if(e instanceof ExecutionException)
                throw (ExecutionException)e;
            else if(e instanceof InterruptedException)
                throw (InterruptedException)e;
            else throw new ExecutionException(e);
        } finally {
            resetContext(impl, prepared);
            closeOperationContext(opContext);
        }
    }

    private void closeOperationContext(SpliceOperationContext opContext) throws ExecutionException {
        if(opContext!=null){
            try {
                opContext.close();
            } catch (IOException e) {
                throw new ExecutionException(e);
            } catch (StandardException e) {
                throw new ExecutionException(e);
            }
        }
    }

    private void resetContext(SpliceTransactionResourceImpl impl, boolean prepared) {
        if(prepared){
            impl.resetContextManager();
        }
        if (impl != null) {
            impl.cleanup();
        }
    }

    private String getTransactionId() {
        final TaskStatus taskStatus = getTaskStatus();
        if (taskStatus != null) {
            return taskStatus.getTransactionId();
        } else {
            return null;
        }
    }

    @Override
    public boolean isCancelled() throws ExecutionException {
        return status.getStatus()==Status.CANCELLED;
    }

		@Override
		public int getPriority() {
				if(instructions==null)
						instructions = SpliceUtils.getSpliceObserverInstructions(scan);
				//TODO -sf- make this also add priority values to favor shorter tasks over longer ones
				//TODO -sf- detect system table operations and give them a different priority
				return SchedulerPriorities.INSTANCE.getBasePriority(instructions.getTopOperation().getClass());
		}

		public HRegion getRegion() {
        return region;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        scan.write(out);
				out.writeByte(hashBucket);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        scan  = new Scan();
        scan.readFields(in);
				hashBucket = in.readByte();
    }

    @Override
    protected String getTaskType() {
        if(instructions==null)
            instructions = SpliceUtils.getSpliceObserverInstructions(scan);
        return instructions.getTopOperation().getClass().getSimpleName();
    }
}
