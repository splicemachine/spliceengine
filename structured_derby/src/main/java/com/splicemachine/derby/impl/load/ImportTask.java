package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.stats.IOStats;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
		public void doExecute() throws ExecutionException, InterruptedException {
				try{
						ExecRow row = getExecRow(importContext);

						long rowsRead = 0l;
						long startTime = System.currentTimeMillis();
						long stopTime;
						try{
								reader.setup(fileSystem,importContext);
								if(importer==null){
										importer = getImporter(row);
								}
								try{
										String[] nextRow;
										do{
												SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
												nextRow = reader.nextRow();
												rowsRead++;

												if(nextRow!=null)
														importer.process(nextRow);
										}while(nextRow!=null);
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

										if(importContext.shouldRecordStats()){
												OperationRuntimeStats runtimeStats = new OperationRuntimeStats(statementId, operationId,
																Bytes.toLong(getTaskId()),region.getRegionNameAsString(),12);

												runtimeStats.addMetric(OperationMetric.START_TIMESTAMP,startTime);
												runtimeStats.addMetric(OperationMetric.STOP_TIMESTAMP,stopTime);
												IOStats readStats = reader.getStats();
												runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,readStats.getBytes());
												runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,readStats.getRows());
												TimeView readView = readStats.getTime();
												runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,readView.getWallClockTime());
												runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,readView.getCpuTime());
												runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,readView.getUserTime());

												IOStats writeStats = importer.getStats();
												runtimeStats.addMetric(OperationMetric.WRITE_BYTES,writeStats.getBytes());
												runtimeStats.addMetric(OperationMetric.WRITE_ROWS,writeStats.getRows());
												TimeView writeView = writeStats.getTime();
												runtimeStats.addMetric(OperationMetric.WRITE_WALL_TIME,writeView.getWallClockTime());
												runtimeStats.addMetric(OperationMetric.WRITE_CPU_TIME,writeView.getCpuTime());
												runtimeStats.addMetric(OperationMetric.WRITE_USER_TIME,writeView.getUserTime());

												SpliceDriver.driver().getTaskReporter().report(importContext.getXplainSchema(),runtimeStats);
										}
								}
						}catch(Exception e){
								if(e instanceof ExecutionException)
										throw (ExecutionException)e;
								throw new ExecutionException(e);
						}
				} catch (StandardException e) {
						throw new ExecutionException(e);
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

		protected ExecRow getExecRow(ImportContext context) throws StandardException {
				ColumnContext[] cols = context.getColumnInformation();
				int size = cols[cols.length-1].getColumnNumber()+1;
				ExecRow row = new ValueRow(size);
				for(ColumnContext col:cols){
						DataValueDescriptor dataValueDescriptor = getDataValueDescriptor(col);
						row.setColumn(col.getColumnNumber()+1, dataValueDescriptor);
				}
				return row;
		}

		private DataValueDescriptor getDataValueDescriptor(ColumnContext columnContext) throws StandardException {
				DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnContext.getColumnType());
				if(!columnContext.isNullable())
						td = td.getNullabilityType(false);
				return td.getNull();
		}

		public ImportContext getContext() {
			return importContext;
		}
}
