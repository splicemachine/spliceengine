package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsJob implements CoprocessorJob {
    private List<Pair<byte[],byte[]>> regions;
    private HTableInterface table;

    private StatisticsTask taskTemplate;
    private TxnView txn;

    public StatisticsJob(List<Pair<byte[], byte[]>> regions,
                         HTableInterface table,
                         StatisticsTask taskTemplate,
                         TxnView txn) {
        this.regions = regions;
        this.table = table;
        this.taskTemplate = taskTemplate;
        this.txn = txn;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        Map<RegionTask,Pair<byte[],byte[]>> task = new IdentityHashMap<>(regions.size());
        for(Pair<byte[],byte[]> region:regions){
            task.put(taskTemplate.getClone(),region);
        }
        return task;
    }

    @Override public HTableInterface getTable() { return table; }
    @Override public byte[] getDestinationTable() { return table.getTableName(); }

    @Override
    public String getJobId() {
        return taskTemplate.getJobId();
    }

    @Override public TxnView getTxn() { return txn; }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    public void invalidateStatisticsCache() throws ExecutionException{
        if(!StatisticsStorage.isStoreRunning()) return; //nothing to invalidate
        long conglomId = Long.parseLong(new String(table.getTableName()));
        StatisticsStorage.getPartitionStore().invalidateCachedStatistics(conglomId);
    }
}
