package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.ForeignKeyCheckWriteHandler;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyCheckWriteFactory implements LocalWriteFactory {

    private int constraintColumnCount;

    ForeignKeyCheckWriteFactory(int constraintColumnCount) {
        this.constraintColumnCount = constraintColumnCount;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyCheckWriteHandler(ctx.getTransactionalRegion(), ctx.getCoprocessorEnvironment(), constraintColumnCount));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}