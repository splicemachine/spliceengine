package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.management.XplainTaskReporter;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.pipeline.api.CallBufferFactory;
import com.splicemachine.pipeline.api.RecordingCallBuffer;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/13/13
 */
public class TableOperationSink implements OperationSink {
    private static final Logger LOG = Logger.getLogger(TableOperationSink.class);
    private static String hostName; 
    		
    
    static {
    		try {
    		hostName = InetAddress.getLocalHost().getHostName(); //TODO -sf- this may not be correct -jl- no clue
    		} catch (UnknownHostException uhe) {
    			hostName = "Unknown";
    			throw new RuntimeException(uhe);
    		}

    }
    		
    /**
     * A chain of tasks for identifying parent and child tasks. The last byte[] in
     * the list is the immediate parent of other tasks.
     */
    public static final ThreadLocal<List<byte[]>> taskChain = new ThreadLocal<>();
    private final CallBufferFactory<KVPair> tableWriter;
    private final SinkingOperation operation;
    private final byte[] taskId;

    private final Timer totalTimer;
    private final long waitTimeNs;
    private long statementId;
		private TxnView txn;
    private byte[] destinationTable;

		public TableOperationSink(byte[] taskId,
                                  SinkingOperation operation,
                                  CallBufferFactory<KVPair> tableWriter,
                                  TxnView txn,
                                  long statementId,
                                  long waitTimeNs, byte[] destinationTable) {
        this.tableWriter = tableWriter;
        this.taskId = taskId;
        this.operation = operation;
				this.txn = txn;
        this.destinationTable = destinationTable;
            //we always record this time information, because it's cheap relative to the per-row timing
        this.totalTimer = Metrics.newTimer();
        this.statementId = statementId;
        this.waitTimeNs = waitTimeNs;
    }

    @Override
    public TaskStats sink(SpliceRuntimeContext spliceRuntimeContext) throws Exception {
        boolean isTemp = isTempTable(destinationTable,spliceRuntimeContext);
				//add ourselves to the task id list
				List<byte[]> bytes = taskChain.get();
				if(bytes==null){
						bytes = Lists.newLinkedList(); //LL used to avoid wasting space here
						taskChain.set(bytes);
				}
				bytes.add(taskId);
        RecordingCallBuffer<KVPair> writeBuffer;
				long rowsRead = 0;
				long rowsWritten = 0;
				Timer writeTimer = spliceRuntimeContext.newTimer();
				KeyEncoder keyEncoder = operation.getKeyEncoder(spliceRuntimeContext);
				DataHash rowHash = operation.getRowHash(spliceRuntimeContext);

				KVPair.Type dataType = operation instanceof UpdateOperation? KVPair.Type.UPDATE: KVPair.Type.INSERT;
				dataType = operation instanceof DeleteOperation? KVPair.Type.DELETE: dataType;
        PairEncoder encoder;
        final boolean[] usedTempBuckets;
        if(isTemp){
            final SpreadBucket tempSpread = SpliceDriver.driver().getTempTable().getCurrentSpread();
            usedTempBuckets = new boolean[tempSpread.getNumBuckets()];
            encoder = new PairEncoder(keyEncoder,rowHash,dataType){
                @Override
                public KVPair encode(ExecRow execRow) throws StandardException, IOException {
                    byte[] key = keyEncoder.getKey(execRow);
                    usedTempBuckets[tempSpread.bucketIndex(key[0])] = true;
                    rowEncoder.setRow(execRow);
                    byte[] row = rowEncoder.encode();

                    return new KVPair(key,row,pairType);
                }
            };
        }else{
            usedTempBuckets = null;
				    encoder = new PairEncoder(keyEncoder,rowHash,dataType);
        }
        try{
            TxnView txn = getTxn(spliceRuntimeContext, destinationTable);
						writeBuffer = operation.transformWriteBuffer(tableWriter.writeBuffer(destinationTable, txn,spliceRuntimeContext));

            ExecRow row;

						totalTimer.startTiming();
						do{
								SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
								row = operation.getNextSinkRow(spliceRuntimeContext);
                if(row==null) continue;

								rowsRead++;
								writeTimer.startTiming();
								KVPair encode = encoder.encode(row);
								writeBuffer.add(encode);
								writeTimer.tick(1);
								rowsWritten++;

            }while(row!=null);

						writeTimer.startTiming();
            writeBuffer.flushBuffer();
            writeBuffer.close();
						writeTimer.stopTiming();

						//stop timing events. We do this inside of the try block because we don't care
						//if the task fails for some reason
						totalTimer.stopTiming();
						if(spliceRuntimeContext.shouldRecordTraceMetrics()){
								long taskIdLong = taskId!=null? Bytes.toLong(taskId): SpliceDriver.driver().getUUIDGenerator().nextUUID();
								List<OperationRuntimeStats> operationStats = OperationRuntimeStats.getOperationStats(operation,
												taskIdLong,statementId,writeBuffer.getWriteStats(),writeTimer.getTime(),spliceRuntimeContext);
								XplainTaskReporter reporter = SpliceDriver.driver().getTaskReporter();
								for(OperationRuntimeStats operationStat:operationStats){
										operationStat.addMetric(OperationMetric.TASK_QUEUE_WAIT_WALL_TIME,waitTimeNs);
										operationStat.setHostName(hostName);

										reporter.report(operationStat,this.txn);
								}
						}
        } catch (Exception e) {
						//unwrap interruptedExceptions
						@SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = Throwables.getRootCause(e);
						if(t instanceof InterruptedException)
								throw (InterruptedException)t;
						else
								throw e;
				}finally{
						Closeables.closeQuietly(encoder);
						bytes = taskChain.get();
						bytes.remove(bytes.size()-1);
						if(bytes.size()<=0){
								taskChain.remove();
						}
						operation.close();

						if(LOG.isDebugEnabled()){
								LOG.debug(String.format("Read %d rows from operation %s",rowsRead,operation.getClass().getSimpleName()));
								LOG.debug(String.format("Wrote %d rows from operation %s",rowsWritten,operation.getClass().getSimpleName()));
						}
        }
        //DB-2007/DB-2070. This is in place to make DB-2007 changes appear unchanged even though it isn't
        if (operation instanceof DMLWriteOperation)
            rowsWritten += ((DMLWriteOperation) operation).getFilteredRows();
        return new TaskStats(totalTimer.getTime().getWallClockTime(), rowsRead, rowsWritten, usedTempBuckets);
    }

    private boolean isTempTable(byte[] destinationTable,SpliceRuntimeContext context){
        return Bytes.equals(destinationTable,context.getTempTable().getTempTableName());
    }

		private TxnView getTxn(SpliceRuntimeContext spliceRuntimeContext, byte[] destinationTable) {
				byte[] tempTableBytes = spliceRuntimeContext.getTempTable().getTempTableName();
				if(Bytes.equals(destinationTable, tempTableBytes)){
						/*
						 * We are writing to the TEMP Table.
						 *
						 * The timestamp has a useful meaning in the TEMP table, which is that
						 * it should be the longified version of the job id (to facilitate dropping
						 * data from TEMP efficiently--See TempTablecompactionScanner for more information).
						 *
						 * However, timestamps can't be negative, so we just take the time portion of the
						 * uuid out and stringify that
						 */
						long l = Snowflake.timestampFromUUID(Bytes.toLong(operation.getUniqueSequenceId()));
						return new ActiveWriteTxn(l,l,Txn.ROOT_TRANSACTION);
				}
        return txn;
		}

//		private String getTransactionId(SpliceRuntimeContext context, byte[] destinationTable) {
//				byte[] tempTableBytes = context.getTempTable().getTempTableName();
//				if(Bytes.equals(destinationTable, tempTableBytes)){
//						/*
//						 * We are writing to the TEMP Table.
//						 *
//						 * The timestamp has a useful meaning in the TEMP table, which is that
//						 * it should be the longified version of the job id (to facilitate dropping
//						 * data from TEMP efficiently--See TempTablecompactionScanner for more information).
//						 *
//						 * However, timestamps can't be negative, so we just take the time portion of the
//						 * uuid out and stringify that
//						 */
//						return Long.toString(Snowflake.timestampFromUUID(Bytes.toLong(operation.getUniqueSequenceId())));
//				}
//				return txn == null ? operation.getTxn() : txn;
//		}

}
