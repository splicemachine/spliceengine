package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.ddl.TentativeDDLDesc;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/15
 */
public class AddColumnTask extends ZkTask {

    private static final long serialVersionUID = 1L;

    private DDLChange ddlChange;

    public AddColumnTask() {}

    public AddColumnTask(String jobId, DDLChange ddlChange) {
        super(jobId, OperationJob.operationTaskPriority, null);
        this.ddlChange = ddlChange;
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        try {
            TentativeDDLDesc tentativeAddColumnDesc = ddlChange.getTentativeDDLDesc();
            WriteContextFactory contextFactory = WriteContextFactoryManager.getWriteContext(tentativeAddColumnDesc
                                                                                                .getBaseConglomerateNumber());
            try {
                contextFactory.addDDLChange(ddlChange);
            } finally {
                contextFactory.close();
            }
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    @Override
    protected String getTaskType() {
        return "AddColumnTask";
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public RegionTask getClone() {
        throw new UnsupportedOperationException("Should not clone AddColumnTasks!");
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(AddColumnTask.class);
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
}
