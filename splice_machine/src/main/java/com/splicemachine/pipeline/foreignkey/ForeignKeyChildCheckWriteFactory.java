package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyChildCheckWriteFactory implements LocalWriteFactory{
    private final FKConstraintInfo fkConstraintInfo;

    ForeignKeyChildCheckWriteFactory(FKConstraintInfo fkConstraintInfo) {
        this.fkConstraintInfo = fkConstraintInfo;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildCheckWriteHandler(ctx.getTransactionalRegion(),fkConstraintInfo));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
    }

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return false;
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        throw new UnsupportedOperationException();
    }

}