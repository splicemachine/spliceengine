package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.writecontext.PipelineWriteContext;
import com.splicemachine.pipeline.writehandler.foreignkey.ForeignKeyChildInterceptWriteHandler;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyInterceptWriteHandler -- see that class for details.
 */
class ForeignKeyChildInterceptWriteFactory implements LocalWriteFactory {

    private final byte[] parentTableName;
    private final ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor;

    ForeignKeyChildInterceptWriteFactory(byte[] parentTableName, ForeignKeyConstraintDescriptor foreignKeyConstraintDescriptor) {
        this.parentTableName = parentTableName;
        this.foreignKeyConstraintDescriptor = foreignKeyConstraintDescriptor;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildInterceptWriteHandler(parentTableName, foreignKeyConstraintDescriptor));
    }

    @Override
    public long getConglomerateId() {
        return 0;
    }

}