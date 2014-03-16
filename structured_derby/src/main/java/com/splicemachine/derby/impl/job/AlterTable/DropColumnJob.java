package com.splicemachine.derby.impl.job.AlterTable;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/11/14
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */

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

public class DropColumnJob implements CoprocessorJob {
    private final HTableInterface table;
    private final long oldConglomId;
    private final long newConglomId;
    private final DDLChange ddlChange;
    private final String txnId;


    public DropColumnJob(HTableInterface table,
                         long oldConglomId,
                         long newConglomId,
                         DDLChange ddlChange) {
        this.table = table;
        this.oldConglomId = oldConglomId;
        this.newConglomId = newConglomId;
        this.ddlChange = ddlChange;
        this.txnId = ddlChange.getTransactionId();
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        DropColumnTask task = new DropColumnTask(getJobId(), oldConglomId, newConglomId, ddlChange);
        return Collections.singletonMap(task, Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return "dropColumnJob-"+txnId.substring(txnId.lastIndexOf('/')+1);
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    @Override
    public TransactionId getParentTransaction() {
        return HTransactorFactory.getTransactionManager().transactionIdFromString(txnId);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
