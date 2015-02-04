package com.splicemachine.derby.impl.job.fk;

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
 * See docs in CreateFKTask for details.
 */
public class CreateFkJob implements CoprocessorJob {

    private final HTableInterface table;
    private final TxnView txn;
    private final int[] backingIndexFormatIds;
    private final long referencedConglomerateId;
    private final long referencingBackingIndexConglomId;
    private final String referencedTableName;

    public CreateFkJob(HTableInterface table, TxnView txn, int referencedConglomerateId, int[] backingIndexFormatIds, long referencingBackingIndexConglomId, String referncedTableName) {
        this.table = table;
        this.txn = txn;
        this.backingIndexFormatIds = backingIndexFormatIds;
        this.referencedConglomerateId = referencedConglomerateId;
        this.referencingBackingIndexConglomId = referencingBackingIndexConglomId;
        this.referencedTableName = referncedTableName;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        CreateFkTask task = new CreateFkTask(getJobId(), backingIndexFormatIds, referencedConglomerateId, referencingBackingIndexConglomId, referencedTableName);
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
        return null;
    }

    @Override
    public TxnView getTxn() {
        return txn;
    }
}
