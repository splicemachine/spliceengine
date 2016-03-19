package com.splicemachine.derby.impl.load;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.uuid.UUIDGenerator;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.URI;
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

		private long logRowCountInterval = SpliceConstants.importTaskStatusReportingRowCount;   // The row count interval of when to log rows read.  For example, write to the log every 10,000 rows read.
		private long rowsRead = 0l;
		private long previouslyLoggedRowsRead = 0l;  // The number of rows read that was last logged.
		private ImportErrorReporter errorReporter;

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
											long statementId,
											long operationId){
				super(jobId,priority);
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
											byte[] taskId){
				super(jobId, priority);
				this.importContext = importContext;
				this.reader = reader;
				this.importer = importer;
				this.taskId = taskId;
		}

		@Override
		public void doExecute() throws ExecutionException, InterruptedException {
			String importFilePath = getImportFilePathAsString();
			String importTaskPath = getTaskNode();

				try{
						ExecRow row = getExecRow(importContext);

						rowsRead = 0l;
						previouslyLoggedRowsRead = 0l;  // The number of rows read that was last logged.
						long stopTime;
						Timer totalTimer = importContext.shouldRecordStats()? Metrics.newTimer(): Metrics.noOpTimer();
						totalTimer.startTiming();
						long startTime = System.currentTimeMillis();
						RowErrorLogger errorLogger = getErrorLogger();
						errorReporter = getErrorReporter(row.getClone(),errorLogger);
						long maxRecords = importContext.getMaxRecords();  // The maximum number of records to import or check in the file.
						ImportTaskManagementStats jmxStats = ImportTaskManagementStats.getInstance();

						// Initialize our JMX stats.  Set row counts for this importTaskPath (and importFilePath) to 0.
						if (importTaskPath != null) {
							jmxStats.initialize(importTaskPath, importFilePath);
						}

						try{
								errorLogger.open();
								reader.setup(fileSystem,importContext);
								if(importer==null){

										importer = getImporter(row,errorReporter);
								}

								try{
									boolean shouldContinue;
									do{
												SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
												String[][] lines = reader.nextRowBatch();

												shouldContinue = importer.processBatch(lines);
												if(shouldContinue)
														rowsRead+=lines.length;
												else if(lines!=null && lines[0]!=null){
														rowsRead++;
														for(int i=1;i<lines.length;i++){
																if(lines[i]==null){
																		rowsRead+=(i-1);
																		break;
																}
														}
												}

												if ((rowsRead - previouslyLoggedRowsRead) >= logRowCountInterval) {
													if (importTaskPath != null) {
														jmxStats.setImportedRowCount(importTaskPath, rowsRead - errorReporter.errorsReported());
														jmxStats.setBadRowCount(importTaskPath, errorReporter.errorsReported());
													}
													previouslyLoggedRowsRead = rowsRead;
													if (LOG.isDebugEnabled()) {
														SpliceLogUtils.debug(LOG, "Imported %d total rows.  Rejected %d total bad rows.  File is %s.", (rowsRead - errorReporter.errorsReported()), errorReporter.errorsReported(), importFilePath);
														if (LOG.isTraceEnabled()) {
															SpliceLogUtils.trace(LOG, "taskId is %s.  taskPath is %s.", Bytes.toLong(taskId), importTaskPath);
														}
													}
												}

												if (maxRecords > 0 && rowsRead >= maxRecords) {
													shouldContinue = false;
												}

									} while (shouldContinue);

									if (importTaskPath != null) {
										jmxStats.setImportedRowCount(importTaskPath, rowsRead - errorReporter.errorsReported());
										jmxStats.setBadRowCount(importTaskPath, errorReporter.errorsReported());
									}

                                    previouslyLoggedRowsRead = rowsRead;
									if (LOG.isDebugEnabled()) {
										SpliceLogUtils.debug(LOG, "Import task finished.  Imported %d total rows.  Rejected %d total bad rows.  File is %s.", (rowsRead - errorReporter.errorsReported()), errorReporter.errorsReported(), importFilePath);
										if (LOG.isTraceEnabled()) {
											SpliceLogUtils.trace(LOG, "taskId is %s.  taskPath is %s.", Bytes.toLong(taskId), importTaskPath);
										}
									}
								} catch (Exception e) {
										throw new ExecutionException(e);
								} finally{
                                    jmxStats.cleanup(importTaskPath);
										Closeables.closeQuietly(reader);
                    /*
                     * We don't call closeQuietly(importer) here, because
                     * we need to make sure that we get out any IOExceptions
                     * that get thrown
                     */
										try{
												importer.close();
										}finally{
												//close error reporter AFTER importer finishes
												Closeables.closeQuietly(errorReporter);
												Closeables.closeQuietly(errorLogger);
										}
										stopTime = System.currentTimeMillis();
								}
								totalTimer.stopTiming();
								//don't report stats if there's an error
								reportStats(startTime, stopTime,importer.getTotalTime(),totalTimer.getTime());
								TaskStats stats = new TaskStats(stopTime-startTime,rowsRead,
												rowsRead-errorReporter.errorsReported());
								getTaskStatus().setStats(stats);
						}catch(Exception e){
								if(e instanceof ExecutionException)
										throw (ExecutionException)e;
								throw new ExecutionException(e);
						}
				} catch (StandardException e) {
						throw new ExecutionException(e);
				}
		}

		/**
		 * Return the import file path as a String.  This is useful for displaying information about the file actually being imported.
		 *
		 * @return the import file path as a String
		 */
		private String getImportFilePathAsString() {
			String importFilePathStr = "UNKNOWN_IMPORT_FILE_PATH";
			if (importContext != null) {
				Path tmpFilePath = importContext.getFilePath();
				if (tmpFilePath != null) {
					URI tmpUri = tmpFilePath.toUri();
					if (tmpUri != null) {
						String tmpPath = tmpUri.getPath();
						if (tmpPath != null) {
							importFilePathStr = tmpPath;
						}
					}
				}
			}
			return importFilePathStr;
		}

		protected ImportErrorReporter getErrorReporter(ExecRow rowTemplate,RowErrorLogger errorLogger) {
				long maxBadRecords = importContext.getMaxBadRecords();
				if(maxBadRecords<0) return FailAlwaysReporter.INSTANCE;

        KVPair.Type importType = importContext.isUpsert()? KVPair.Type.UPSERT: KVPair.Type.INSERT;
        PairDecoder decoder = ImportUtils.newEntryEncoder(rowTemplate,importContext,getUuidGenerator(),importType).getDecoder(rowTemplate);

				long queueSize = maxBadRecords;
				if(maxBadRecords==0|| maxBadRecords > SpliceConstants.importLogQueueSize)
						queueSize = SpliceConstants.importLogQueueSize;

				QueuedErrorReporter delegate = new QueuedErrorReporter( (int)queueSize,
								SpliceConstants.importLogQueueWaitTimeMs, errorLogger, decoder);
				/*
				 * When maxBadRecords = 0, then we want to log everything and never fail. This is to match
				 * Oracle etc.'s behavior (and thus be more like what people expect). Otherwise, we have a maximum
				 * threshold that we need to adhere to.
				 */
				if(maxBadRecords==0)
						return delegate;
				else
						return new ThresholdErrorReporter(maxBadRecords, delegate);
		}

		protected UUIDGenerator getUuidGenerator() {
				return SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
		}

		protected RowErrorLogger getErrorLogger() throws StandardException {
				if(importContext.getMaxBadRecords()<0)
						return NoopErrorLogger.INSTANCE;

				/*
				 * Made protected so that it can be easily overridden for testing.
				 */
				Path directory = importContext.getBadLogDirectory();
				if(directory==null){
						directory = importContext.getFilePath().getParent();
						//make sure that we can write to this directory, otherwise it's no good either
						ImportUtils.validateWritable(directory,fileSystem,false);
				}

				Path badLogFile = new Path(directory,"_BAD_"+importContext.getFilePath().getName()+"_"+Bytes.toLong(taskId));

				return new FileErrorLogger(fileSystem,badLogFile,128);
		}

		protected void reportStats(long startTimeMs, long stopTimeMs,TimeView processTime,TimeView totalTimeView) throws IOException {
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

						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,readStats.bytesSeen());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,readStats.elementsSeen());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,readView.getWallClockTime());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,readView.getCpuTime());
						runtimeStats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,readView.getUserTime());

						runtimeStats.addMetric(OperationMetric.PROCESSING_WALL_TIME,processTime.getWallClockTime());
						runtimeStats.addMetric(OperationMetric.PROCESSING_CPU_TIME,processTime.getCpuTime());
						runtimeStats.addMetric(OperationMetric.PROCESSING_USER_TIME,processTime.getUserTime());

						WriteStats writeStats = importer.getWriteStats();
						OperationRuntimeStats.addWriteStats(writeStats, runtimeStats);

						SpliceDriver.driver().getTaskReporter().report(runtimeStats,getTxn());
				}
		}

		protected Importer getImporter(ExecRow row,ImportErrorReporter errorReporter) throws ExecutionException {
				boolean shouldParallelize;
				try {
						shouldParallelize = reader.shouldParallelize(fileSystem, importContext);
				} catch (IOException e) {
						throw new ExecutionException(e);
				}

				TxnView txn = status.getTxnInformation();

				if(LOG.isInfoEnabled())
						SpliceLogUtils.info(LOG,"Importing %s using transaction %s, which is a child of transaction %s",
										reader.toString(),txn,txn.getParentTxnView());
        KVPair.Type importType = importContext.isUpsert()? KVPair.Type.UPSERT: KVPair.Type.INSERT;
        if(!shouldParallelize) {
                return new SequentialImporter(importContext,row, txn,
                        SpliceDriver.driver().getTableWriter(),errorReporter,importType);
				} else
						return new ParallelImporter(importContext,row,
                    SpliceConstants.maxImportProcessingThreads,
                    SpliceConstants.maxImportReadBufferSize,txn,errorReporter,importType);
		}

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        byte[] table = importContext.getTableName().getBytes();
        /*
         * We use an additive transaction structure here so that two separate files
         * in the same import process will not throw Write/Write conflicts with one another,
         * but will instead by passed through to the underlying constraint (that way, we'll
         * get UniqueConstraint violations instead of Write/Write conflicts).
         */
        return tc.beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,table);
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
		public RegionTask getClone() {
				throw new UnsupportedOperationException("Should not clone Import tasks!");
		}

		@Override public boolean isSplittable() { return false; }

		@Override
		public int getPriority() {
				return SchedulerPriorities.INSTANCE.getBasePriority(ImportTask.class);
		}

		@Override
		protected String getTaskType() {
				return "importTask";
		}

		@Override
		public void prepareTask(byte[] start, byte[] stop,RegionCoprocessorEnvironment rce,
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
				super.prepareTask(start,stop,rce, zooKeeper);
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
