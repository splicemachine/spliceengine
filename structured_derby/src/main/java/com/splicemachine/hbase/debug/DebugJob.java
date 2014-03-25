package com.splicemachine.hbase.debug;

import java.io.IOException;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.hbase.table.SpliceHTable;
import com.splicemachine.job.Task;
import com.splicemachine.si.impl.TransactionId;

/**
 * @author Scott Fines
 *         Created on: 9/17/13
 */
public abstract class DebugJob implements CoprocessorJob {
    protected final String destinationDirectory;
    private final String tableName;
    private final Configuration config;
    protected String opId;

    protected DebugJob(String tableName, String destinationDirectory, Configuration config) {
        this.tableName = tableName;
        this.destinationDirectory = destinationDirectory;
        this.config = config;
    }

    @Override
    public HTableInterface getTable() {
        try {
            return new SpliceHTable(Bytes.toBytes(tableName), HConnectionManager.createConnection(config),
                                    Executors.newCachedThreadPool(), HBaseRegionCache.getInstance());
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
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey,
                                                                       byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask, Pair.newPair(taskStartKey, taskEndKey));
    }
}
