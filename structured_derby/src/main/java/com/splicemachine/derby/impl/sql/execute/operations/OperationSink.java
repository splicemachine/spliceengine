package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/13/13
 */
public class OperationSink {
    private static final Logger LOG = Logger.getLogger(OperationSink.class);

    public static interface Translator{
        @Nonnull List<Mutation> translate(@Nonnull ExecRow row,byte[] postfix) throws IOException;

        /**
         * @return true if mutations with the same row key should be pushed to the same location in TEMP
         */
        boolean mergeKeys();
    }

    private final WriteCoordinator tableWriter;
    private final SinkingOperation operation;
    private final byte[] taskId;

    private long rowCount = 0;
    private byte[] postfix;

    private OperationSink(byte[] taskId,SinkingOperation operation,WriteCoordinator tableWriter) {
        this.tableWriter = tableWriter;
        this.taskId = taskId;
        this.operation = operation;
    }

    public static OperationSink create(SinkingOperation operation, byte[] taskId) throws IOException {
        //TODO -sf- move this to a static initializer somewhere

        return new OperationSink(taskId,operation,SpliceDriver.driver().getTableWriter());
    }

    public TaskStats sink(byte[] destinationTable) throws Exception {
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();

        CallBuffer<KVPair> writeBuffer;
        byte[] postfix = getPostfix(false);
        try{
            RowEncoder encoder = operation.getRowEncoder();
            encoder.setPostfix(postfix);
            String txnId = operation.getTransactionID();
            writeBuffer = tableWriter.writeBuffer(destinationTable, txnId);

            ExecRow row;
            do{
//                debugFailIfDesired(writeBuffer);

                long start = System.nanoTime();
                //row = operation.getNextRowCore();
                row = operation.getNextSinkRow();
                if(row==null) continue;

                stats.readAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();

                encoder.write(row,writeBuffer);

//                debugFailIfDesired(writeBuffer);

                stats.writeAccumulator().tick(System.nanoTime() - start);
            }while(row!=null);
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) { //TODO -sf- deal with Primary Key and Unique Constraints here
        	SpliceLogUtils.error(LOG, "Error in Operation Sink",e);
            SpliceLogUtils.logAndThrow(LOG, Exceptions.parseException(e));
        }
        return stats.finish();
    }

    private byte[] getPostfix(boolean shouldMakUnique) {
        if(taskId==null && shouldMakUnique)
            return SpliceUtils.getUniqueKey();
        else if(taskId==null)
            postfix = SpliceUtils.getUniqueKey();
        if(postfix == null){
            postfix = new byte[taskId.length+(shouldMakUnique?8:0)];
            System.arraycopy(taskId,0,postfix,0,taskId.length);
        }
        if(shouldMakUnique){
            rowCount++;
            System.arraycopy(Bytes.toBytes(rowCount),0,postfix,taskId.length,8);
        }
        return postfix;
    }

    private void debugFailIfDesired(CallBuffer<Mutation> writeBuffer) throws Exception {
    /*
     * For testing purposes, if the flag FAIL_TASKS_RANDOMLY is set, then randomly decide whether
     * or not to fail this task.
     */
        if(SpliceConstants.debugFailTasksRandomly){
            double shouldFail = Math.random();
            if(shouldFail<SpliceConstants.debugTaskFailureRate){
                //make sure that we flush the buffer occasionally
//                if(Math.random()<2*SpliceConstants.debugTaskFailureRate)
                    writeBuffer.flushBuffer();
                //wait for 1 second, then fail
                try {
                    Thread.sleep(1000l);
                } catch (InterruptedException e) {
                    //we were interrupted! sweet, fail early!
                    throw new IOException(e);
                }

                //now fail with a retryable exception
                throw new IOException("Random task failure as determined by debugFailTasksRandomly");
            }
        }
    }

}
