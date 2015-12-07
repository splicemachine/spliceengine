package com.splicemachine.pipeline.writecontextfactory;

import com.google.common.primitives.Longs;
import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyChildInterceptWriteHandler;
import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyChildInterceptWriteFactory implements LocalWriteFactory {

    /* The base-table or unique-index conglomerate that this FK references. */
    private final long referencedConglomerateNumber;
    private final FKConstraintInfo fkConstraintInfo;

    ForeignKeyChildInterceptWriteFactory(long referencedConglomerateNumber, FKConstraintInfo fkConstraintInfo) {
        this.referencedConglomerateNumber = referencedConglomerateNumber;
        this.fkConstraintInfo = fkConstraintInfo;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildInterceptWriteHandler(referencedConglomerateNumber, fkConstraintInfo));
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
}