package com.splicemachine.pipeline.foreignkey;

import org.sparkproject.guava.primitives.Longs;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyChildInterceptWriteFactory implements LocalWriteFactory{

    /* The base-table or unique-index conglomerate that this FK references. */
    private final long referencedConglomerateNumber;
    private final FKConstraintInfo fkConstraintInfo;
    private final PipelineExceptionFactory exceptionFactory;

    ForeignKeyChildInterceptWriteFactory(long referencedConglomerateNumber, FKConstraintInfo fkConstraintInfo,
                                         PipelineExceptionFactory exceptionFactory) {
        this.referencedConglomerateNumber = referencedConglomerateNumber;
        this.fkConstraintInfo = fkConstraintInfo;
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildInterceptWriteHandler(referencedConglomerateNumber, fkConstraintInfo,exceptionFactory));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
    }



    @Override
    public int hashCode() {
        return Longs.hashCode(this.referencedConglomerateNumber);
    }

    // Equality is based on the referenced conglomerate.  Within the FK backing index where the WriteHandlers from
    // this factory will be installed we only need one WriteHandler for each referenced conglomerate number.
    @Override
    public boolean equals(Object o) {
        return o == this || (o instanceof ForeignKeyChildInterceptWriteFactory) &&
                ((ForeignKeyChildInterceptWriteFactory)o).referencedConglomerateNumber == this.referencedConglomerateNumber;
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