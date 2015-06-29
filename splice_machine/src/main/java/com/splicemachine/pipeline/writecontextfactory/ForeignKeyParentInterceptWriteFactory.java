package com.splicemachine.pipeline.writecontextfactory;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyParentInterceptWriteHandler;

import java.io.IOException;
import java.util.List;

/**
 * LocalWriteFactory for ForeignKeyParentInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyParentInterceptWriteFactory implements LocalWriteFactory {

    private final String parentTableName;
    private final List<Long> referencingIndexConglomerateNumbers = Lists.newArrayList();

    ForeignKeyParentInterceptWriteFactory(String parentTableName, List<Long> referencingIndexConglomerateNumbers) {
        this.parentTableName = parentTableName;
        this.referencingIndexConglomerateNumbers.addAll(referencingIndexConglomerateNumbers);
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyParentInterceptWriteHandler(parentTableName, referencingIndexConglomerateNumbers));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
    }

    /**
     * If a FK is dropped or the child table is dropped remove it from the list of conglomerates we check.
     */
    public void removeReferencingIndexConglomerateNumber(long conglomerateNumber) {
        this.referencingIndexConglomerateNumbers.remove(conglomerateNumber);
    }

}