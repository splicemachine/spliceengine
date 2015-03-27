package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

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
    private final DDLChange ddlChange;
    private final int expectedScanReadWidth;
    private final long demarcationTimestamp;


    public PopulateConglomerateJob(HTableInterface table,
                                   int expectedScanReadWidth,
                                   long demarcationTimestamp,
                                   DDLChange ddlChange) {
        this.table = table;
        this.expectedScanReadWidth = expectedScanReadWidth;
        this.demarcationTimestamp = demarcationTimestamp;
        this.ddlChange = ddlChange;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        ZkTask task = new PopulateConglomerateTask(getJobId(),
                                                   expectedScanReadWidth,
                                                   demarcationTimestamp,
                                                   ddlChange);
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
        return getClass().getSimpleName()+"-"+ddlChange.getTxn();
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
