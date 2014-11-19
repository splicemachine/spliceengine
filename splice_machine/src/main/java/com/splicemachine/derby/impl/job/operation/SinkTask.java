package com.splicemachine.derby.impl.job.operation;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.SpliceTransactionView;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.Status;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionalRegions;
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
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Common remote "task" logic for all SinkingOperations.  Operation-specific sinking logic is implemented in OperationSink
 * instances, which are invoked by this class.
 *
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class SinkTask extends ZkTask {
	private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
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
    private boolean isTraced = false;

		/*Stats stuff*/

		/**
     * Serialization Constructor.
     */
    @SuppressWarnings("UnusedDeclaration")
		public SinkTask(){ super(); }

    public SinkTask(String jobId,
                    Scan scan,
                    byte[] parentTaskId,
                    int priority) {
        super(jobId, priority);
        this.scan = scan;
        this.parentTaskId = parentTaskId;
    }

		@Override
		public RegionTask getClone() {
				Scan scanCopy = new Scan();
				scanCopy.setStopRow(scan.getStopRow());
				scanCopy.setStartRow(scan.getStartRow());
				scanCopy.setFamilyMap(scan.getFamilyMap());
				scanCopy.setBatch(scan.getBatch());
				scanCopy.setCaching(scan.getCaching());
				scanCopy.setCacheBlocks(scan.getCacheBlocks());
				for(Map.Entry<String,byte[]>attribute:scan.getAttributesMap().entrySet()){
						scanCopy.setAttribute(attribute.getKey(),attribute.getValue());
				}
        SinkTask sinkTask = new SinkTask(jobId, scanCopy, parentTaskId, getPriority());
        sinkTask.setParentTxnInformation(getParentTxn());
        return sinkTask;
		}


    @Override public boolean isSplittable() { return true; }

		@Override
    public void prepareTask(byte[] start, byte[] end,RegionCoprocessorEnvironment rce,SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        //make sure that our task id is properly set
				adjustScan(start,end);

        this.region = rce.getRegion();
        super.prepareTask(start,end,rce, zooKeeper);
    }


		private void adjustScan(byte[] start, byte[] end) {
				byte[] scanStart =scan.getStartRow();
				if(scanStart==null||scanStart.length<=0||Bytes.compareTo(scanStart,start)<0){
						scan.setStartRow(start);
				}

				byte[] scanStop = scan.getStopRow();
				if(scanStop==null||scanStop.length<=0||Bytes.compareTo(end,scanStop)>0)
						scan.setStopRow(end);

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
				Activation activation = null;
				try{
						transactionResource = new SpliceTransactionResourceImpl();
						transactionResource.prepareContextManager();
						prepared=true;
						if(instructions==null)
								instructions = SpliceUtils.getSpliceObserverInstructions(scan);
            setupTransaction();
            transactionResource.marshallTransaction(getTxn(), instructions);
            activation = instructions.getActivation(transactionResource.getLcc());
            if(activation.isTraced()){
                Txn txn = getTxn();
                if(!txn.allowsWrites()){
                    elevateTransaction("xplain".getBytes());
                }
            }
            instructions.setTxn(getTxn());
            SpliceTransactionManager stm = (SpliceTransactionManager)transactionResource.getLcc().getTransactionExecute();
            ((SpliceTransactionView)stm.getRawTransaction()).setTxn(getTxn());

						spliceRuntimeContext = instructions.getSpliceRuntimeContext();
						spliceRuntimeContext.markAsSink();
						spliceRuntimeContext.setCurrentTaskId(getTaskId());
						SpliceOperation op = instructions.getTopOperation();
						if(op.shouldRecordStats()){
								spliceRuntimeContext.recordTraceMetrics();
						}

            TransactionalRegion txnRegion = TransactionalRegions.get(region);
            opContext = new SpliceOperationContext(region, txnRegion,
                    scan,activation,
										instructions.getStatement(),
										transactionResource.getLcc(),
										true,instructions.getTopOperation(), spliceRuntimeContext,getTxn());
						//init the operation stack

						op.init(opContext);
				}catch(Exception e){
						closeQuietly(prepared, transactionResource, opContext);
						throw new ExecutionException(e);
				}
        waitTimeNs = System.nanoTime()-prepareTimestamp;
        try{
            doExecute();
        }finally{
            taskWatcher.setTask(null);
            if (activation != null) {
            	try {
					activation.close();
				} catch (StandardException e) {
				}
            	activation = null;
            }
        }
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
        try {
			SpliceOperation op = instructions.getTopOperation();
            Txn txn = getTxn();
            if(LOG.isTraceEnabled()) {
                SpliceLogUtils.trace(LOG, "Sink[%s]: %s over [%s,%s)", Bytes.toLong(getTaskId()), txn, BytesUtil.toHex(scan.getStartRow()), BytesUtil.toHex(scan.getStopRow()));
            }
            OperationSink opSink = OperationSinkFactory.create((SinkingOperation) op, getTaskId(), txn, op.getStatementId(), waitTimeNs);
            TaskStats stats = opSink.sink(spliceRuntimeContext);
            status.setStats(stats);

            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"Sink[%s]: %s sunk %d rows",Bytes.toLong(getTaskId()),txn,stats.getTotalRowsWritten());
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
		protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
				byte[] table = null;
        boolean additive = false;
        SpliceOperation topOperation = instructions.getTopOperation();
        if(topOperation instanceof DMLWriteOperation){
						table = ((DMLWriteOperation) topOperation).getDestinationTable();
            if(topOperation instanceof InsertOperation){
		            /*
		             * (DB-949)Insert operations should use an Additive transaction, so that internal WW conflicts will
		             * be replaced by the proper UniqueConstraint violations (if necessary)
		             */
                additive = true;
            }
				}
        Txn txn = tc.beginChildTransaction(parentTxn, parentTxn.getIsolationLevel(), additive, table);
        SpliceLogUtils.trace(LOG,"Executing task %s with transaction %s, which is a child of %s",Bytes.toLong(taskId),txn,parentTxn);
        return txn;
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
        derbyFactory.writeScanExternal(out, scan);
		out.writeByte(hashBucket);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        scan = derbyFactory.readScanExternal(in);
		hashBucket = in.readByte();
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
}
