package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyChildCheckWriteHandler;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyChildCheckWriteFactory implements LocalWriteFactory {

    private final ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor;

    ForeignKeyChildCheckWriteFactory(ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor) {
        this.foreignKeyConstraintDescriptor = foreignKeyConstraintDescriptor;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildCheckWriteHandler(ctx.getTransactionalRegion(), ctx.getCoprocessorEnvironment(), foreignKeyConstraintDescriptor));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}