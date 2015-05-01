package com.splicemachine.pipeline.writecontextfactory;

import java.io.IOException;

import com.splicemachine.derby.ddl.DDLChangeType;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.SnapshotIsolatedWriteHandler;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.si.impl.HTransactorFactory;

public class AlterTableWriteFactory implements LocalWriteFactory {
    private final DDLChange ddlChange;

    private AlterTableWriteFactory(DDLChange ddlChange) {
        this.ddlChange = ddlChange;
    }

    public static AlterTableWriteFactory create(DDLChange ddlChange) {
        DDLChangeType changeType = ddlChange.getChangeType();
        if (changeType == DDLChangeType.ADD_COLUMN ||
            changeType == DDLChangeType.ADD_PRIMARY_KEY ||
            changeType == DDLChangeType.ADD_UNIQUE_CONSTRAINT ||
            changeType == DDLChangeType.DROP_CONSTRAINT ||
            changeType == DDLChangeType.DROP_PRIMARY_KEY ||
            changeType == DDLChangeType.DROP_COLUMN) {
            return new AlterTableWriteFactory(ddlChange);
        } else {
            // should only happen when implementing a new DDLChangeType but
            // forgetting to add it here
            throw new RuntimeException("Unknown DDLChangeType: "+changeType);
        }
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {

        RowTransformer transformer = ((TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc()).createRowTransformer();
        WriteHandler writeHandler =
            ((TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc()).createWriteHandler(transformer);

        DDLFilter ddlFilter = HTransactorFactory.getTransactionReadController().newDDLFilter(ddlChange.getTxn());
        ctx.addLast(new SnapshotIsolatedWriteHandler(writeHandler, ddlFilter));
    }

    @Override
    public long getConglomerateId() {
        return ddlChange.getTentativeDDLDesc().getConglomerateNumber();
    }
}
