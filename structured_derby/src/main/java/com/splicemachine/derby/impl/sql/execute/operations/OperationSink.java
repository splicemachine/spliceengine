package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.stats.Timer;
import com.splicemachine.stats.Timers;
import com.splicemachine.utils.Snowflake;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/13/13
 */
public class OperationSink {
    private static final Logger LOG = Logger.getLogger(OperationSink.class);

		/**
		 * A chain of tasks for identifying parent and child tasks. The last byte[] in
		 * the list is the immediate parent of other tasks.
		 */
		public static final ThreadLocal<List<byte[]>> taskChain = new ThreadLocal<List<byte[]>>();
    private final CallBufferFactory<KVPair> tableWriter;
    private final SinkingOperation operation;
    private final byte[] taskId;
    private final String transactionId;

		private final Timer totalTimer;

		public OperationSink(byte[] taskId,
												 SinkingOperation operation,
												 CallBufferFactory<KVPair> tableWriter,
												 String transactionId) {
        this.tableWriter = tableWriter;
        this.taskId = taskId;
        this.operation = operation;
        this.transactionId = transactionId;
				//we always record this time information, because it's cheap relative to the per-row timing
				this.totalTimer = Timers.newTimer();
    }

    public static OperationSink create(SinkingOperation operation, byte[] taskId, String transactionId) throws IOException {
        return new OperationSink(taskId,operation,SpliceDriver.driver().getTableWriter(), transactionId);
    }

    public TaskStats sink(byte[] destinationTable, SpliceRuntimeContext spliceRuntimeContext) throws Exception {
				//add ourselves to the task id list
				List<byte[]> bytes = taskChain.get();
				if(bytes==null){
						bytes = Lists.newLinkedList(); //LL used to avoid wasting space here
						taskChain.set(bytes);
				}
				bytes.add(taskId);
        CallBuffer<KVPair> writeBuffer;
				PairEncoder encoder;
				long rowsRead = 0;
				long rowsWritten = 0;
        try{
						KeyEncoder keyEncoder = operation.getKeyEncoder(spliceRuntimeContext);
						DataHash rowHash = operation.getRowHash(spliceRuntimeContext);

						KVPair.Type dataType = operation instanceof UpdateOperation? KVPair.Type.UPDATE: KVPair.Type.INSERT;
						dataType = operation instanceof DeleteOperation? KVPair.Type.DELETE: dataType;
						encoder = new PairEncoder(keyEncoder,rowHash,dataType);
            String txnId = getTransactionId(destinationTable);
						writeBuffer = operation.transformWriteBuffer(tableWriter.writeBuffer(destinationTable, txnId));

            ExecRow row;

						totalTimer.startTiming();
						do{
								SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
								row = operation.getNextSinkRow(spliceRuntimeContext);
                if(row==null) continue;

								rowsRead++;
								writeBuffer.add(encoder.encode(row));
								rowsWritten++;

            }while(row!=null);

            writeBuffer.flushBuffer();
            writeBuffer.close();

						//stop timing events. We do this inside of the try block because we don't care
						//if the task fails for some reason
						totalTimer.stopTiming();
        } catch (Exception e) {
						//unwrap interruptedExceptions
						@SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = Throwables.getRootCause(e);
						if(t instanceof InterruptedException)
								throw (InterruptedException)t;
						else
								throw e;
				}finally{
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
				return new TaskStats(totalTimer.getWallClockTime(),rowsRead,rowsWritten);
    }

		private String getTransactionId(byte[] destinationTable) {
				byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
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
						return Long.toString(Snowflake.timestampFromUUID(Bytes.toLong(operation.getUniqueSequenceId())));
				}
				return transactionId == null ? operation.getTransactionID() : transactionId;
		}

}
