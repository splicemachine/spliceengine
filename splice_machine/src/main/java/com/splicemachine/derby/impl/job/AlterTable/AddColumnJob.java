package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.derby.ddl.TentativeAddColumnDesc;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.si.api.TxnView;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/15
 */
public class AddColumnJob implements CoprocessorJob {

    private final HTableInterface table;
    private final DDLChange ddlChange;


    public AddColumnJob(HTableInterface table, DDLChange ddlChange) {
        this.table = table;
        this.ddlChange = ddlChange;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        ZkTask task = new AddColumnTask(getJobId(), ddlChange);
        return Collections.singletonMap(task, Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public byte[] getDestinationTable() {
        return null;
    }

    @Override
    public String getJobId() {
        return getClass().getSimpleName()+"-"+ddlChange.getTxn().getTxnId();
    }

    @Override
    public TxnView getTxn() {
        return ddlChange.getTxn();
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[]
        taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }
}
