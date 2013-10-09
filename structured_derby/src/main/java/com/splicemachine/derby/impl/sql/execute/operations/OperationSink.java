package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.CallBufferFactory;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.WriteCoordinator;
import com.splicemachine.utils.Snowflake;
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

    private final CallBufferFactory<KVPair> tableWriter;
    private final SinkingOperation operation;
    private final byte[] taskId;
    private final String transactionId;
    private final Snowflake.Generator uuidGenerator;

    private long rowCount = 0;
    private byte[] postfix;

    public OperationSink(byte[] taskId,
                         SinkingOperation operation,
                          CallBufferFactory<KVPair> tableWriter,
                          String transactionId,
                          Snowflake.Generator snowflakeGenerator) {
        this.tableWriter = tableWriter;
        this.taskId = taskId;
        this.operation = operation;
        this.transactionId = transactionId;
        this.uuidGenerator = snowflakeGenerator;
    }

    public static OperationSink create(SinkingOperation operation, byte[] taskId, String transactionId) throws IOException {
        //TODO -sf- move this to a static initializer somewhere

        return new OperationSink(taskId,operation,SpliceDriver.driver().getTableWriter(), transactionId,
                SpliceDriver.driver().getUUIDGenerator().newGenerator(100));
    }

    public TaskStats sink(byte[] destinationTable, SpliceRuntimeContext spliceRuntimeContext) throws Exception {
    	TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();

        CallBuffer<KVPair> writeBuffer;
        byte[] postfix = getPostfix(false);
        RowEncoder encoder = null;
        try{
            encoder = operation.getRowEncoder(spliceRuntimeContext);
            encoder.setPostfix(postfix);
            String txnId = transactionId == null ? operation.getTransactionID() : transactionId;
            writeBuffer = tableWriter.writeBuffer(destinationTable, txnId);

            ExecRow row;
            long rowsRead = 0l;
            long rowsWritten = 0l;

            do{
                long start = 0l;
                if(stats.readAccumulator().shouldCollectStats()){
                    start = System.nanoTime();
                }
                row = operation.getNextSinkRow(spliceRuntimeContext);
                if(row==null) continue;

                if(stats.readAccumulator().shouldCollectStats()){
                    stats.readAccumulator().tick(System.nanoTime()-start);
                }else{
                    rowsRead++;
                }

                if(stats.writeAccumulator().shouldCollectStats()){
                    start = System.nanoTime();
                }

                encoder.write(row,writeBuffer);

//                debugFailIfDesired(writeBuffer);

                if(stats.writeAccumulator().shouldCollectStats()){
                    stats.writeAccumulator().tick(System.nanoTime() - start);
                }else{
                    rowsWritten++;
                }

            }while(row!=null);

            if( !stats.writeAccumulator().shouldCollectStats() ){
                stats.writeAccumulator().tickRecords(rowsWritten);
            }

            if( !stats.readAccumulator().shouldCollectStats() ){
                stats.readAccumulator().tickRecords(rowsRead);
            }


            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) { //TODO -sf- deal with Primary Key and Unique Constraints here
        	SpliceLogUtils.error(LOG, "Error in Operation Sink",e);
            throw e;
        }finally{
            if(encoder!=null)
                encoder.close();
        }
        return stats.finish();
    }

    private byte[] getPostfix(boolean shouldMakUnique) {
        if(taskId==null && shouldMakUnique)
            return uuidGenerator.nextBytes();
        else if(taskId==null)
            postfix = uuidGenerator.nextBytes();
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
}
