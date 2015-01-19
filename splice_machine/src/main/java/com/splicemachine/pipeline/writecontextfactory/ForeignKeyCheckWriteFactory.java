package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.ForeignKeyCheckWriteHandler;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyCheckWriteFactory implements LocalWriteFactory {

    /* formatIds for just the columns in the FK  */
    private int[] formatIds;

    ForeignKeyCheckWriteFactory(int[] formatIds) {
        this.formatIds = formatIds;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyCheckWriteHandler(ctx.getTransactionalRegion(), ctx.getCoprocessorEnvironment(), formatIds));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}