package com.splicemachine.derby.impl.job.operation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceDriver;
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
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

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
		private SpliceRuntimeContext spliceRuntimeContext;
		private SpliceTransactionResourceImpl transactionResource;
		private SpliceOperationContext opContext;

		/*Stats stuff*/

		/**
     * Serialization Constructor.
     */
    @SuppressWarnings("UnusedDeclaration")
		public SinkTask(){ super(); }

    public SinkTask(String jobId,
                    Scan scan,
                    String transactionId,
                    byte[] parentTaskId,
                    boolean readOnly,
                    int priority) {
        super(jobId, priority, transactionId, readOnly);
        this.scan = scan;
        this.parentTaskId = parentTaskId;
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
		public void execute() throws ExecutionException, InterruptedException {
				/*
				 * we initialize so that later, if we need to use initialization information
				 * during, say, transaction creation, then we are sufficiently initialized
				 * to do it without being weird.
				 *
				 * This makes the flow of events:
				 *
				 * 1. initialize
				 * 2. create the child transaction
				 * 3. execute work
				 */
				boolean prepared = false;
				try{
						transactionResource = new SpliceTransactionResourceImpl();
						transactionResource.prepareContextManager();
						prepared=true;
						if(instructions==null)
								instructions = SpliceUtils.getSpliceObserverInstructions(scan);
						transactionResource.marshallTransaction(instructions);
						Activation activation = instructions.getActivation(transactionResource.getLcc());
						spliceRuntimeContext = instructions.getSpliceRuntimeContext();
						spliceRuntimeContext.markAsSink();
						spliceRuntimeContext.setCurrentTaskId(getTaskId());
						SpliceOperation op = instructions.getTopOperation();
						if(op.shouldRecordStats()){
								spliceRuntimeContext.recordTraceMetrics();
								spliceRuntimeContext.setXplainSchema(op.getXplainSchema());
						}

						opContext = new SpliceOperationContext(region,
										scan,activation,instructions.getStatement(), transactionResource.getLcc(),true,instructions.getTopOperation(), spliceRuntimeContext);
						//init the operation stack

						op.init(opContext);
				}catch(Exception e){
						closeQuietly(prepared, transactionResource, opContext);
						throw new ExecutionException(e);
				}
				super.execute();
		}

		protected void closeQuietly(boolean prepared, SpliceTransactionResourceImpl impl, SpliceOperationContext opContext)  {
				try{
						resetContext(impl,prepared);
						closeOperationContext(opContext);
				} catch (ExecutionException e) {
						LOG.error("Unable to close Operation context during unexpected initialization error",e);
				}
		}

		@Override
    public void doExecute() throws ExecutionException, InterruptedException {
				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"executing task %s",Bytes.toString(getTaskId()));
        try {
						SpliceOperation op = instructions.getTopOperation();
            OperationSink opSink = OperationSink.create((SinkingOperation) op, getTaskId(), getTransactionId(), op.getStatementId(),waitTimeNs);

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
        }finally{
						//if we get this far, then we were initialized
						resetContext(transactionResource,true);
						closeOperationContext(opContext);
				}
    }

		@Override
		protected TransactionId beginChildTransaction(TransactionManager transactor, TransactionId parent) throws IOException {
				byte[] table = null;
				if(instructions.getTopOperation() instanceof DMLWriteOperation){
						table = ((DMLWriteOperation)instructions.getTopOperation()).getDestinationTable();
				}
				return transactor.beginChildTransaction(parent, !readOnly, table);
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

    // FIXME: Part of old Writable interface - use protoBuf
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				byte[] bytes = ProtobufUtil.toScan(scan).toByteArray();
				out.writeByte(hashBucket);
				out.writeInt(bytes.length);
				out.write(bytes);
//        scan.write(out);
		}

    // FIXME: Part of old Writable interface - use protoBuf
		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				hashBucket = in.readByte();

				byte[] scanBytes = new byte[in.readInt()];
				in.readFully(scanBytes);
				ClientProtos.Scan scan1 = ClientProtos.Scan.parseFrom(scanBytes);
				scan = ProtobufUtil.toScan(scan1);
		}

    @Override
    protected String getTaskType() {
        if(instructions==null)
            instructions = SpliceUtils.getSpliceObserverInstructions(scan);
        return instructions.getTopOperation().getClass().getSimpleName();
    }

/*******************************************************************************************************/
		/*private helper methods*/
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
}
