package com.splicemachine.derby.impl.job.fk;

import com.google.common.base.Throwables;
import com.splicemachine.derby.ddl.AddForeignKeyDDLDescriptor;
import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

/**
 * Adds FK ParentCheck and ParentIntercept write handlers to the parent table in a new FK relationship. This task is
 * for handling the the case where we have created a new child table with FK and the parent table already exists AND the
 * WriteContexts for that table may have already been initialized (otherwise the would just be setup properly during
 * initialization based on FK info in the data dictionary).
 */
public class CreateFkTask extends ZkTask {

    private static final long serialVersionUID = 1L;

    private TxnView txn;
    private AddForeignKeyDDLDescriptor ddlDescriptor;
    /* This is the number of the conglomerate we want to modify. Will be the child table's backing index if this
     * job is for modifying write contexts of the child.  Will be the parent base table or unique index conglomerate
     * if this job is modifying write contexts of the parent. */
    private long jobTargetConglomerateNumber;

    public CreateFkTask() {
    }

    public CreateFkTask(String jobId, TxnView txn, long jobTargetConglomerateNumber, AddForeignKeyDDLDescriptor ddlDescriptor) {
        super(jobId, OperationJob.operationTaskPriority, null);
        this.txn = txn;
        this.jobTargetConglomerateNumber = jobTargetConglomerateNumber;
        this.ddlDescriptor = ddlDescriptor;
    }

    @Override
    public RegionTask getClone() {
        throw new UnsupportedOperationException("Should not clone CreateFkTask!");
    }

    @Override
    public boolean isSplittable() {
        return false;
    }

    @Override
    protected String getTaskType() {
        return "CreateFkTask";
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try {
            WriteContextFactory contextFactory = WriteContextFactoryManager.getWriteContext(jobTargetConglomerateNumber);
            try {
                DDLChange ddlChange = new DDLChange(txn, DDLChangeType.ADD_FOREIGN_KEY, ddlDescriptor);
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
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(CreateFkTask.class);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(txn);
        out.writeLong(jobTargetConglomerateNumber);
        out.writeObject(ddlDescriptor);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.txn = (TxnView) in.readObject();
        this.jobTargetConglomerateNumber = in.readLong();
        this.ddlDescriptor = (AddForeignKeyDDLDescriptor) in.readObject();
    }
}