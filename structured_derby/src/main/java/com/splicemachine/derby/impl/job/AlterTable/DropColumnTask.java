package com.splicemachine.derby.impl.job.AlterTable;

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
import com.splicemachine.derby.ddl.TentativeDropColumnDesc;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 3/11/14
 * Time: 4:57 PM
 * To change this template use File | Settings | File Templates.
 */
public class DropColumnTask extends ZkTask {

    private DDLChange ddlChange;
    private long oldConglomId;
    private long newConglomId;

    public DropColumnTask(){}
    public DropColumnTask(String jobId,
                          long oldConglomId,
                          long newConglomId,
                          DDLChange ddlChange) {
        super(jobId, OperationJob.operationTaskPriority,ddlChange.getTransactionId(),false);
        this.oldConglomId = oldConglomId;
        this.newConglomId = newConglomId;
        this.ddlChange = ddlChange;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected String getTaskType() {
        return "DropColumnTask";
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }
    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try{
            TentativeDropColumnDesc tentativeDropColumnDesc = (TentativeDropColumnDesc)ddlChange.getTentativeDDLDesc();
            LocalWriteContextFactory contextFactory = SpliceIndexEndpoint.getContextFactory(tentativeDropColumnDesc.getBaseConglomerateNumber());
            contextFactory.addDDLChange(ddlChange);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(DropColumnTask.class);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(ddlChange);
        out.writeLong(oldConglomId);
        out.writeLong(newConglomId);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        ddlChange = (DDLChange) in.readObject();
        oldConglomId = in.readLong();
        newConglomId = in.readLong();
    }
}
