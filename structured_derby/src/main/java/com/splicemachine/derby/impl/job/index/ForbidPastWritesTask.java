package com.splicemachine.derby.impl.job.index;

import com.google.common.base.Throwables;
import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.derby.ddl.TentativeIndexDesc;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

public class ForbidPastWritesTask extends ZkTask {
    private static final long serialVersionUID = 5l;

    private DDLChange ddlChange;

    public ForbidPastWritesTask() { }

    public ForbidPastWritesTask(String jobId,
                                DDLChange ddlChange) {
        super(jobId, OperationJob.operationTaskPriority,ddlChange.getTransactionId(),false);
        this.ddlChange = ddlChange;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected String getTaskType() {
        return "forbidPastWritesTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(ddlChange);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ddlChange = (DDLChange) in.readObject();
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
            contextFactory.addIndex(ddlChange);
        } catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(ForbidPastWritesTask.class);
    }
}
