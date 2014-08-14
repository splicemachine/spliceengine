package com.splicemachine.derby.impl.job.index;

import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class CreateIndexJob implements CoprocessorJob{
    private final HTableInterface table;
    private final DDLChange ddlChange;
    private int[] columnOrdering;
    private int[] formatIds;

    public CreateIndexJob(HTableInterface table,
                          DDLChange ddlChange,
                          int[] columnOrdering,
                          int[] formatIds) {
        this.table = table;
        this.ddlChange = ddlChange;
        this.columnOrdering = columnOrdering;
        this.formatIds = formatIds;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        CreateIndexTask task = new CreateIndexTask(getJobId(), ddlChange, columnOrdering, formatIds);
        return Collections.singletonMap(task,Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        TxnView txn = ddlChange.getTxn();
        return "indexJob-"+txn.getTxnId();
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    @Override
    public byte[] getDestinationTable() {
        return null;
    }

    @Override
    public TxnView getTxn() {
        return ddlChange.getTxn();
    }
}
