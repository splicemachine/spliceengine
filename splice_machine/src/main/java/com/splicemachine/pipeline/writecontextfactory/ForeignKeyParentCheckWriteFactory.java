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
    /* Version of parent table serializers ('1.0', '2.0', etc) */
    private String parentTableVersion;

    ForeignKeyParentCheckWriteFactory(int[] formatIds, String parentTableVersion) {
        this.formatIds = formatIds;
        this.parentTableVersion = parentTableVersion;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyParentCheckWriteHandler(ctx.getTransactionalRegion(), formatIds, parentTableVersion));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}