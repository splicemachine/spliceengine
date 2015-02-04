package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyParentInterceptWriteHandler;

import java.io.IOException;
import java.util.List;

/**
 * LocalWriteFactory for ForeignKeyParentInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyParentInterceptWriteFactory implements LocalWriteFactory {

    private final String parentTableName;
    private final List<Long> referencingIndexConglomerateIds;

    ForeignKeyParentInterceptWriteFactory(String parentTableName, List<Long> referencingIndexConglomerateIds) {
        this.parentTableName = parentTableName;
        this.referencingIndexConglomerateIds = referencingIndexConglomerateIds;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyParentInterceptWriteHandler(parentTableName, referencingIndexConglomerateIds));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}