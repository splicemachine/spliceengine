package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyChildCheckWriteHandler;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyChildCheckWriteFactory implements LocalWriteFactory {

    private final FKConstraintInfo fkConstraintInfo;

    ForeignKeyChildCheckWriteFactory(FKConstraintInfo fkConstraintInfo) {
        this.fkConstraintInfo = fkConstraintInfo;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildCheckWriteHandler(ctx.getTransactionalRegion(), ctx.getCoprocessorEnvironment(), fkConstraintInfo));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
    }

}