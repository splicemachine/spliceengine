package com.splicemachine.derby.impl.job.index;

import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
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
public class ForbidPastWritesJob implements CoprocessorJob{
    private final HTableInterface table;
    private final DDLChange ddlChange;
    private final String transactionId;

    public ForbidPastWritesJob(HTableInterface table,
                               DDLChange ddlChange) {
        this.table = table;
        this.transactionId = ddlChange.getTransactionId();
        this.ddlChange = ddlChange;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        ForbidPastWritesTask task = new ForbidPastWritesTask(getJobId(), ddlChange);
        return Collections.singletonMap(task,Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return "forbidPastWritesJob-"+transactionId.substring(transactionId.lastIndexOf('/')+1);
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    @Override
    public TransactionId getParentTransaction() {
        return HTransactorFactory.getTransactorControl().transactionIdFromString(transactionId);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
