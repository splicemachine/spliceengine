package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.si.api.TxnView;

/**
 * @author Jeff Cunningham
 *         Date: 3/19/15
 */
public class PopulateConglomerateJob implements CoprocessorJob {
    private final HTableInterface table;
    private final TxnView txn;
    private final String tableVersion;
    private final long newConglomId;
    private final long oldConglomId;
    private final ColumnInfo[] columnInfos;
    private final int[] columnOrdering;
    private final long demarcationTimestamp;


    public PopulateConglomerateJob(HTableInterface table,
                                   TxnView txn,
                                   String tableVersion,
                                   long newConglomId,
                                   long oldConglomId,
                                   ColumnInfo[] columnInfos,
                                   int[] columnOrdering,
                                   long demarcationTimestamp) {
        this.table = table;
        this.txn = txn;
        this.tableVersion = tableVersion;
        this.newConglomId = newConglomId;
        this.oldConglomId = oldConglomId;
        this.columnInfos = columnInfos;
        this.columnOrdering = columnOrdering;
        this.demarcationTimestamp = demarcationTimestamp;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        ZkTask task = new PopulateConglomerateTask(tableVersion,
                                                   newConglomId,
                                                   oldConglomId,
                                                   getJobId(),
                                                   columnInfos,
                                                   columnOrdering,
                                                   demarcationTimestamp);
        return Collections.singletonMap(task, Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public byte[] getDestinationTable() {
        return new byte[0];
    }

    @Override
    public String getJobId() {
        return getClass().getSimpleName()+"-"+txn;
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[]
        taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }
}
