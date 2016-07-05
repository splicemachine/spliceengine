package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.ddl.DDLMessage;
import org.sparkproject.guava.collect.Lists;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;

import java.io.IOException;
import java.util.List;

/**
 * LocalWriteFactory for ForeignKeyParentInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyParentInterceptWriteFactory implements LocalWriteFactory{

    private final String parentTableName;
    private final List<Long> referencingIndexConglomerateNumbers = Lists.newArrayList();
    private final PipelineExceptionFactory exceptionFactory;
    private final List<DDLMessage.FKConstraintInfo> constraintInfos = Lists.newArrayList();

    ForeignKeyParentInterceptWriteFactory(String parentTableName,
                                          List<Long> referencingIndexConglomerateNumbers,
                                          PipelineExceptionFactory exceptionFactory, List<DDLMessage.FKConstraintInfo> fkConstraintInfo) {
        this.parentTableName = parentTableName;
        this.exceptionFactory=exceptionFactory;
        this.referencingIndexConglomerateNumbers.addAll(referencingIndexConglomerateNumbers);
        this.constraintInfos.addAll(fkConstraintInfo);
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyParentInterceptWriteHandler(parentTableName, referencingIndexConglomerateNumbers,exceptionFactory,constraintInfos));
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

    @Override
    public boolean canReplace(LocalWriteFactory newContext){
        return false;
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        throw new UnsupportedOperationException();
    }
}