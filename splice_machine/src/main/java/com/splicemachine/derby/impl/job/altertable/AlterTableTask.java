package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;
import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.ddl.TentativeAddConstraintDesc;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
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
public class AlterTableTask extends ZkTask {

    private static final long serialVersionUID = 1L;

    private DDLChange ddlChange;

    public AlterTableTask() {}

    public AlterTableTask(String jobId, DDLChange ddlChange) {
        super(jobId, SpliceConstants.operationTaskPriority, null);
        this.ddlChange = ddlChange;
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        try {
            TentativeDDLDesc tentativeAddColumnDesc = ddlChange.getTentativeDDLDesc();
            WriteContextFactory contextFactory =
                WriteContextFactoryManager.getWriteContext(tentativeAddColumnDesc.getBaseConglomerateNumber());
            try {
                if (ddlChange.getChangeType() == DDLChangeType.DROP_CONSTRAINT) {
                    // For drop constraint task, we just need to remove the constraint index
                    long indexConglomId = ((TentativeAddConstraintDesc)ddlChange.getTentativeDDLDesc()).getIndexConglomerateId();
                    contextFactory.dropIndex(indexConglomId, ddlChange.getTxn());
                } else {
                    contextFactory.addDDLChange(ddlChange);
                }
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
        return getClass().getSimpleName();
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
        throw new UnsupportedOperationException("Should not clone AlterTableTasks!");
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(AlterTableTask.class);
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
