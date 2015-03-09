package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyParentCheckWriteHandler;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyParentCheckWriteFactory implements LocalWriteFactory {

    /* formatIds for just the columns in the FK  */
    private final int[] formatIds;

    ForeignKeyParentCheckWriteFactory(int[] formatIds) {
        this.formatIds = formatIds;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyParentCheckWriteHandler(ctx.getTransactionalRegion(), formatIds));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}