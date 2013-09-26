package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.job.Task;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 9/17/13
 */
public abstract class DebugJob implements CoprocessorJob{
    private final String tableName;
    protected final String destinationDirectory;
    protected String opId;
    private final Configuration config;

    protected DebugJob(String tableName, String destinationDirectory, Configuration config) {
        this.tableName = tableName;
        this.destinationDirectory = destinationDirectory;
        this.config = config;
    }

    @Override
    public HTableInterface getTable() {
        try {
            return new HTable(config, tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TransactionId getParentTransaction() {
        return null;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public String getJobId() {
        return opId;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }
}
