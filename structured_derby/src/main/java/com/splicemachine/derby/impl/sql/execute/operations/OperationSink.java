package com.splicemachine.derby.impl.sql.execute.operations;

import com.gotometrics.orderly.StringRowKey;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.TableWriter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/13/13
 */
public class OperationSink {
    public static interface Translator{
        @Nonnull List<Mutation> translate(@Nonnull ExecRow row) throws IOException;
    }
    private final TableWriter tableWriter;
    private final SpliceOperation operation;
    private final byte[] taskId;
    private final byte[] taskIdCol;
    private static final Logger LOG = Logger.getLogger(OperationSink.class);

    public OperationSink(byte[] taskIdCol,byte[] taskId,SpliceOperation operation,TableWriter tableWriter) {
        this.tableWriter = tableWriter;
        this.operation = operation;
        this.taskId = taskId;
        this.taskIdCol = taskIdCol;
    }

    public static OperationSink create(SpliceOperation operation, byte[] taskId) throws IOException {
        //TODO -sf- move this to a static initializer somewhere
        byte[] taskIdCol = new StringRowKey().serialize(SpliceConstants.TASK_ID_COL);

        return new OperationSink(taskIdCol,taskId,operation,SpliceDriver.driver().getTableWriter());
    }

    public static OperationSink create(SpliceOperation operation,
                                       TableWriter writer,byte[] taskId) throws IOException {
        //TODO -sf- move this to a static initializer somewhere
        byte[] taskIdCol = new StringRowKey().serialize(SpliceConstants.TASK_ID_COL);

        return new OperationSink(taskIdCol,taskId,operation,writer);
    }

    public TaskStats sink(byte[] destinationTable) throws IOException {
        TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
        stats.start();

        Translator translator = operation.getTranslator();

        boolean isTempTable = Arrays.equals(destinationTable,SpliceConstants.TEMP_TABLE_BYTES);
        CallBuffer<Mutation> writeBuffer;
        try{
            writeBuffer = tableWriter.writeBuffer(destinationTable);

            ExecRow row;
            do{
                long start = System.nanoTime();
                row = operation.getNextRowCore();
                if(row==null) continue;

                stats.readAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                List<Mutation> mutations = translator.translate(row);


                /*
                 * Here we add the taskId column to the temp table writes, so that
                 * if a task fails and must be retried, we can filter out any rows which
                 * have already been written.
                 */
                for(Mutation mutation:mutations){
                    if(isTempTable && mutation instanceof Put && taskIdCol!=null)
                        ((Put)mutation).add(SpliceConstants.DEFAULT_FAMILY_BYTES,taskIdCol,taskId);

                    writeBuffer.add(mutation);
                }

                stats.writeAccumulator().tick(System.nanoTime()-start);
            }while(row!=null);
            writeBuffer.flushBuffer();
            writeBuffer.close();
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, Exceptions.getIOException(e));
        }
        return stats.finish();
    }

}
