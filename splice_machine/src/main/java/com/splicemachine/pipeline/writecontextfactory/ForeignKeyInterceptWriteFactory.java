package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.ForeignKeyInterceptWriteHandler;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyInterceptWriteFactory implements LocalWriteFactory {

    private byte[] parentTableName;
    private ConstraintContext constraintContext;

    ForeignKeyInterceptWriteFactory(byte[] parentTableName, ConstraintContext constraintContext) {
        this.parentTableName = parentTableName;
        this.constraintContext = constraintContext;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyInterceptWriteHandler(parentTableName, constraintContext));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}