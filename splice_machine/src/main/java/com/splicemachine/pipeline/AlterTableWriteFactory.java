package com.splicemachine.pipeline;

import java.io.IOException;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.pipeline.writehandler.WriteHandler;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.impl.DDLFilter;

/**
 * The write intercepting side of alter table.<br/>
 * Intercepts writes to old conglom and forwards them to new.
 */
public class AlterTableWriteFactory implements LocalWriteFactory{
    private final DDLMessage.DDLChange ddlChange;
    private final AlterTableDDLDescriptor ddlDescriptor;
    private final TransactionReadController readController;

    private AlterTableWriteFactory(DDLMessage.DDLChange ddlChange,
                                   AlterTableDDLDescriptor ddlDescriptor,
                                   TransactionReadController readController) {
        this.ddlChange = ddlChange;
        this.ddlDescriptor = ddlDescriptor;
        this.readController = readController;
    }

    public static AlterTableWriteFactory create(DDLMessage.DDLChange ddlChange,
                                                TransactionReadController readController,
                                                PipelineExceptionFactory exceptionFactory) {
        DDLMessage.DDLChangeType changeType = ddlChange.getDdlChangeType();
        if (changeType == DDLMessage.DDLChangeType.ADD_COLUMN)
            return new AlterTableWriteFactory(ddlChange,new TentativeAddColumnDesc(ddlChange.getTentativeAddColumn(),exceptionFactory),readController);
        else if (changeType == DDLMessage.DDLChangeType.ADD_UNIQUE_CONSTRAINT)
            return new AlterTableWriteFactory(ddlChange,new TentativeAddConstraintDesc(ddlChange.getTentativeAddConstraint(),exceptionFactory),readController);
        else if (changeType == DDLMessage.DDLChangeType.DROP_PRIMARY_KEY)
            return new AlterTableWriteFactory(ddlChange,new TentativeDropPKConstraintDesc(ddlChange.getTentativeDropPKConstraint(),exceptionFactory),readController);
        else if (changeType == DDLMessage.DDLChangeType.DROP_COLUMN)
            return new AlterTableWriteFactory(ddlChange,new TentativeDropColumnDesc(ddlChange.getTentativeDropColumn(),exceptionFactory),readController);
        else {
            throw new RuntimeException("Unknown DDLChangeType: "+changeType);
        }
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        RowTransformer transformer = ddlDescriptor.createRowTransformer();
        WriteHandler writeHandler = ddlDescriptor.createWriteHandler(transformer);
        DDLFilter ddlFilter = readController.newDDLFilter(DDLUtils.getLazyTransaction(ddlChange.getTxnId()));
        ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
    }

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return false;
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        throw new UnsupportedOperationException();
    }

    @Override
    public long getConglomerateId() {
        return ddlDescriptor.getConglomerateNumber();
    }
}
