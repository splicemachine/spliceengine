package com.splicemachine.derby.impl.job.fk;

import com.splicemachine.derby.ddl.FKTentativeDDLDesc;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * See docs in FKTask for details.
 */
public class FkJob implements CoprocessorJob {

    private final HTableInterface table;
    /* Transaction that is adding a FK */
    private final TxnView txn;
    /* Info necessary to create the FK */
    private final FKTentativeDDLDesc ddlDescriptor;
    /* Run this task on all regions for this conglomerate number  */
    private final long jobTargetConglomerateNumber;
    /* Used to indicate if this job is adding or dropping a FK */
    private final DDLChangeType ddlChangeType;

    public FkJob(HTableInterface table, TxnView txn, long targetConglomerateNumber, DDLChangeType ddlChangeType,
                 FKTentativeDDLDesc ddlDescriptor) {
        this.table = table;
        this.txn = txn;
        this.jobTargetConglomerateNumber = targetConglomerateNumber;
        this.ddlChangeType = ddlChangeType;
        this.ddlDescriptor = ddlDescriptor;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        FkTask task = new FkTask(getJobId(), txn, jobTargetConglomerateNumber, ddlChangeType, ddlDescriptor);
        return Collections.singletonMap(task, Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return "CreateFkJob-" + txn.getTxnId();
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask, Pair.newPair(taskStartKey, taskEndKey));
    }

    @Override
    public byte[] getDestinationTable() {
        return this.table.getName().getName();
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }
}
