package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.table.SpliceHTable;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.Txn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.concurrent.Executors;

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
						return new SpliceHTable(Bytes.toBytes(tableName), HConnectionManager.getConnection(config), Executors.newCachedThreadPool(), HBaseRegionCache.getInstance());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getJobId() {
        return opId;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    @Override public byte[] getDestinationTable() { return null; }

    @Override public Txn getTxn() { return null; }
}
