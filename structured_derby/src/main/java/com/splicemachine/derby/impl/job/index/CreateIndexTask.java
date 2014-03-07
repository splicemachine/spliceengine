package com.splicemachine.derby.impl.job.index;

import com.google.common.base.Throwables;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class CreateIndexTask extends ZkTask {
    private static final long serialVersionUID = 5l;

    private DDLChange ddlChange;
    private int[] columnOrdering;
    private int[] formatIds;

    public CreateIndexTask() { }

    public CreateIndexTask(String jobId,
                           DDLChange ddlChange,
                           int[] columnOrdering,
                           int[] formatIds) {
        super(jobId, OperationJob.operationTaskPriority,ddlChange.getTransactionId(),false);
        this.ddlChange = ddlChange;
        this.columnOrdering = columnOrdering;
        this.formatIds = formatIds;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected String getTaskType() {
        return "createIndexTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(ddlChange);
        int n = (columnOrdering != null) ? columnOrdering.length : 0;
        out.writeInt(n);
        for (int i = 0; i < n; ++i) {
            out.writeInt(columnOrdering[i]);
        }

        n = (formatIds != null) ? formatIds.length : 0;
        out.writeInt(n);
        for (int i = 0; i < n; ++i) {
            out.writeInt(formatIds[i]);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ddlChange = (DDLChange) in.readObject();
        int n = in.readInt();
        if (n > 0)
            columnOrdering = new int[n];
        for (int i = 0; i < n; ++i) {
            columnOrdering[i] = in.readInt();
        }

        n = in.readInt();
        if (n > 0)
            formatIds = new int[n];
        for (int i = 0; i < n; ++i) {
            formatIds[i] = in.readInt();
        }
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }
    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try{
            //add index to table watcher
            TentativeIndexDesc tentativeIndexDesc = ddlChange.getTentativeIndexDesc();
            LocalWriteContextFactory contextFactory = SpliceIndexEndpoint.getContextFactory(tentativeIndexDesc.getBaseConglomerateNumber());
            contextFactory.addIndex(ddlChange, columnOrdering, formatIds);
        } catch (Exception e) {
        	SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    @Override
    public int getPriority() {
            return SchedulerPriorities.INSTANCE.getBasePriority(CreateIndexTask.class);
    }
}
