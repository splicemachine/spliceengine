package com.splicemachine.derby.impl.job.fk;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;
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

    /* formatIds for the backing index of the FK we are creating */
    private int[] backingIndexFormatIds;
    /* conglom ID of unique index or base table primary key our FK references */
    private long referencedConglomerateId;
    /* conglom ID of the backing-index associated with the FK */
    private long referencingConglomerateId;
    /* users visible name of the table new FK references */
    private String referencedTableName;


    public CreateFkTask() {
    }

    public CreateFkTask(String jobId, int[] backingIndexFormatIds, long referencedConglomerateId, long referencingConglomerateId, String referencedTableName) {
        super(jobId, OperationJob.operationTaskPriority, null);
        this.backingIndexFormatIds = backingIndexFormatIds;
        this.referencedConglomerateId = referencedConglomerateId;
        this.referencingConglomerateId = referencingConglomerateId;
        this.referencedTableName = referencedTableName;
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
            WriteContextFactory contextFactory = WriteContextFactoryManager.getWriteContext(referencedConglomerateId);
            try {
                contextFactory.addForeignKeyParentCheckWriteFactory(backingIndexFormatIds);
                contextFactory.addForeignKeyParentInterceptWriteFactory(referencedTableName, ImmutableList.of(referencingConglomerateId));
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
        out.writeLong(referencedConglomerateId);
        out.writeLong(referencingConglomerateId);
        out.writeUTF(referencedTableName);
        out.writeInt(backingIndexFormatIds.length);
        for (int formatId : backingIndexFormatIds) {
            out.writeInt(formatId);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.referencedConglomerateId = in.readLong();
        this.referencingConglomerateId = in.readLong();
        this.referencedTableName = in.readUTF();
        int n = in.readInt();
        if (n > 0) {
            backingIndexFormatIds = new int[n];
            for (int i = 0; i < n; ++i) {
                backingIndexFormatIds[i] = in.readInt();
            }
        }
    }
}
