package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.stats.IOStats;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 8/26/13
 */
public class ImportTask extends ZkTask{
		private static final long serialVersionUID = 2l;
		private static final Logger LOG = Logger.getLogger(ImportTask.class);

		protected ImportContext importContext;
		protected FileSystem fileSystem;
		protected ImportReader reader;

		private Importer importer;
		private long statementId;
		private long operationId;

		/**
		 * @deprecated only available for a a no-args constructor
		 */
		@Deprecated
		@SuppressWarnings("UnusedDeclaration")
		public ImportTask() { }

		public ImportTask(String jobId,
											ImportContext importContext,
											ImportReader reader,
											int priority,
											String parentTxnId,
											long statementId,
											long operationId){
				super(jobId,priority,parentTxnId,false);
				this.importContext = importContext;
				this.reader = reader;
				this.statementId = statementId;
				this.operationId = operationId;
		}

		public ImportTask(String jobId,
											ImportContext importContext,
											ImportReader reader,
											Importer importer,
											int priority,
											String parentTxnId){
				super(jobId, priority, parentTxnId, false);
				this.importContext = importContext;
				this.reader = reader;
				this.importer = importer;
		}

		@Override
		protected TransactionId beginChildTransaction(TransactionManager transactor, TransactionId parent) throws IOException {
				byte[] table = Long.toString(importContext.getTableId()).getBytes();
				return transactor.beginChildTransaction(parent,!readOnly,table);
		}

		@Override
		public void doExecute() throws ExecutionException, InterruptedException {
				try{
						ExecRow row = getExecRow(importContext);

						long rowsRead = 0l;
						long stopTime;
						Timer totalTimer = importContext.shouldRecordStats()? Metrics.newTimer(): Metrics.noOpTimer();
						totalTimer.startTiming();
						long startTime = System.currentTimeMillis();
						try{
								reader.setup(fileSystem,importContext);
								if(importer==null){
										importer = getImporter(row);
								}

								try{
										boolean shouldContinue;
										do{
												SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
												String[][] lines = reader.nextRowBatch();

												shouldContinue = importer.processBatch(lines);
										}while(shouldContinue);
								} catch (Exception e) {
										throw new ExecutionException(e);
								} finally{
										Closeables.closeQuietly(reader);
                    /*
                     * We don't call closeQuietly(importer) here, because
                     * we need to make sure that we get out any IOExceptions
                     * that get thrown
                     */
										importer.close();
										stopTime = System.currentTimeMillis();
								}
								totalTimer.stopTiming();
								//don't report stats if there's an error
								reportStats(startTime, stopTime,importer.getTotalTime(),totalTimer.getTime());
						}catch(Exception e){
								if(e instanceof ExecutionException)
										throw (ExecutionException)e;
								throw new ExecutionException(e);
						}
				} catch (StandardException e) {
						throw new ExecutionException(e);
				}
		}

		protected void reportStats(long startTimeMs, long stopTimeMs,TimeView processTime,TimeView totalTimeView) {
				if(importContext.shouldRecordStats()){
						OperationRuntimeStats runtimeStats = new OperationRuntimeStats(statementId, operationId,
										Bytes.toLong(getTaskId()),region.getRegionNameAsString(),12);

						IOStats readStats = reader.getStats();
						TimeView readView = readStats.getTime();
						runtimeStats.addMetric(OperationMetric.START_TIMESTAMP,startTimeMs);
						runtimeStats.addMetric(OperationMetric.STOP_TIMESTAMP,stopTimeMs);
						runtimeStats.addMetric(OperationMetric.TOTAL_WALL_TIME, totalTimeView.getWallClockTime());
						runtimeStats.addMetric(OperationMetric.TOTAL_CPU_TIME, totalTimeView.getCpuTime());
						runtimeStats.addMetric(OperationMetric.TOTAL_USER_TIME, totalTimeView.getUserTime());
						runtimeStats.addMetric(OperationMetric.TASK_QUEUE_WAIT_WALL_TIME,waitTimeNs);

						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,readStats.getBytes());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,readStats.getRows());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,readView.getWallClockTime());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,readView.getCpuTime());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,readView.getUserTime());

						runtimeStats.addMetric(OperationMetric.PROCESSING_WALL_TIME,processTime.getWallClockTime());
						runtimeStats.addMetric(OperationMetric.PROCESSING_CPU_TIME,processTime.getCpuTime());
						runtimeStats.addMetric(OperationMetric.PROCESSING_USER_TIME,processTime.getUserTime());

						WriteStats writeStats = importer.getWriteStats();
						OperationRuntimeStats.addWriteStats(writeStats, runtimeStats);

						SpliceDriver.driver().getTaskReporter().report(importContext.getXplainSchema(),runtimeStats);
				}
		}

		protected Importer getImporter(ExecRow row) throws ExecutionException {
				boolean shouldParallelize;
				try {
						shouldParallelize = reader.shouldParallelize(fileSystem, importContext);
				} catch (IOException e) {
						throw new ExecutionException(e);
				}
				String transactionId = getTaskStatus().getTransactionId();
				if(LOG.isInfoEnabled())
						SpliceLogUtils.info(LOG,"Importing %s using transaction %s, which is a child of transaction %s",
										reader.toString(),transactionId,parentTxnId);
				if(shouldParallelize) {
						return  new ParallelImporter(importContext,row, transactionId);
				} else
//					return new SequentialImporter(importContext,row,transactionId);
						return new ParallelImporter(importContext,row,1,SpliceConstants.maxImportReadBufferSize,transactionId);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				super.readExternal(in);
				importContext = (ImportContext)in.readObject();
				reader = (ImportReader)in.readObject();
				statementId = in.readLong();
				operationId = in.readLong();
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				super.writeExternal(out);
				out.writeObject(importContext);
				out.writeObject(reader);
				out.writeLong(statementId);
				out.writeLong(operationId);
		}

		@Override
		public boolean invalidateOnClose() {
				return false;
		}

		@Override
		public int getPriority() {
				return SchedulerPriorities.INSTANCE.getBasePriority(ImportTask.class);
		}

		@Override
		protected String getTaskType() {
				return "importTask";
		}

		@Override
		public void prepareTask(RegionCoprocessorEnvironment rce,
														SpliceZooKeeperManager zooKeeper) throws ExecutionException {
				HRegion rceRegion = rce.getRegion();
				if(LOG.isDebugEnabled()){
						byte[] startKey = rceRegion.getStartKey();
						byte[] endKey = rceRegion.getEndKey();
						SpliceLogUtils.debug(LOG,
										"Preparing import for file %s on region with bounds [%s,%s)",
										importContext.getFilePath(),Bytes.toStringBinary(startKey),Bytes.toStringBinary(endKey));
				}
				fileSystem = rceRegion.getFilesystem();
				super.prepareTask(rce, zooKeeper);
		}

		public static ExecRow getExecRow(ImportContext context) throws StandardException {
				ColumnContext[] cols = context.getColumnInformation();
				int size = cols[cols.length-1].getColumnNumber()+1;
				ExecRow row = new ValueRow(size);
				for(ColumnContext col:cols){
						DataValueDescriptor dataValueDescriptor = getDataValueDescriptor(col);
						row.setColumn(col.getColumnNumber()+1, dataValueDescriptor);
				}
				return row;
		}

		public static DataValueDescriptor getDataValueDescriptor(ColumnContext columnContext) throws StandardException {
				DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnContext.getColumnType());
				if(!columnContext.isNullable())
						td = td.getNullabilityType(false);
				return td.getNull();
		}

		public ImportContext getContext() {
			return importContext;
		}
}
