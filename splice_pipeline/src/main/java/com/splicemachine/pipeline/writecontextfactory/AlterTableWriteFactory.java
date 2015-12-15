package com.splicemachine.pipeline.writecontextfactory;

import java.io.IOException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.*;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;

/**
 * The write intercepting side of alter table.<br/>
 * Intercepts writes to old conglom and forwards them to new.
 */
public class AlterTableWriteFactory implements LocalWriteFactory {
    private final DDLMessage.DDLChange ddlChange;
    private final AlterTableDDLDescriptor ddlDescriptor;
    private AlterTableWriteFactory(DDLMessage.DDLChange ddlChange,AlterTableDDLDescriptor ddlDescriptor) {
        this.ddlChange = ddlChange;
        this.ddlDescriptor = ddlDescriptor;
    }

    public static AlterTableWriteFactory create(DDLMessage.DDLChange ddlChange) {
        DDLMessage.DDLChangeType changeType = ddlChange.getDdlChangeType();
        if (changeType == DDLMessage.DDLChangeType.ADD_COLUMN)
            return new AlterTableWriteFactory(ddlChange,new TentativeAddColumnDesc(ddlChange.getTentativeAddColumn()));
        else if (changeType == DDLMessage.DDLChangeType.ADD_UNIQUE_CONSTRAINT)
            return new AlterTableWriteFactory(ddlChange,new TentativeAddConstraintDesc(ddlChange.getTentativeAddConstraint()));
        else if (changeType == DDLMessage.DDLChangeType.DROP_PRIMARY_KEY)
            return new AlterTableWriteFactory(ddlChange,new TentativeDropPKConstraintDesc(ddlChange.getTentativeDropPKConstraint()));
        else if (changeType == DDLMessage.DDLChangeType.DROP_COLUMN)
            return new AlterTableWriteFactory(ddlChange,new TentativeDropColumnDesc(ddlChange.getTentativeDropColumn()));
        else {
            throw new RuntimeException("Unknown DDLChangeType: "+changeType);
        }
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {

        RowTransformer transformer = ddlDescriptor.createRowTransformer();
        WriteHandler writeHandler = ddlDescriptor.createWriteHandler(transformer);
        DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(DDLUtils.getLazyTransaction(ddlChange.getTxnId()));
        ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
    }

    @Override
    public long getConglomerateId() {
        return ddlDescriptor.getConglomerateNumber();
    }
}
