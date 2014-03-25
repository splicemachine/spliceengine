package com.splicemachine.hbase.debug;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.hbase.table.SpliceHTable;
import com.splicemachine.job.Task;
import com.splicemachine.si.impl.TransactionId;

/**
 * @author Scott Fines
 *         Created on: 9/16/13
 */
public class NonTransactionalCounterJob implements CoprocessorJob {
    private final String destinationDirectory;
    private final String table;
    private final String operationId;

    public NonTransactionalCounterJob(String destinationDirectory, String table) {
        this.destinationDirectory = destinationDirectory;
        this.table = table;
        this.operationId = table + ":nonTransactionalCounter";
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        return Collections.singletonMap(new NonTransactionalCounterTask(operationId,
                                                                        1,
                                                                        true,
                                                                        destinationDirectory),
                                        Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW)
        );
    }

    @Override
    public HTableInterface getTable() {
        try {
            return new SpliceHTable(Bytes.toBytes(table), HConnectionManager.createConnection(SpliceConstants.config)
                , Executors.newCachedThreadPool(), HBaseRegionCache.getInstance());
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
        return operationId;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask, Pair.newPair(taskStartKey, taskEndKey));
    }
}
