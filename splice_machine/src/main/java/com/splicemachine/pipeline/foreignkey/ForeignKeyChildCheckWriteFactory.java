package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.ddl.DDLMessage.*;
import com.splicemachine.pipeline.context.PipelineWriteContext;
import com.splicemachine.pipeline.contextfactory.LocalWriteFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;

import java.io.IOException;

/**
 * LocalWriteFactory for ForeignKeyCheckWriteHandler -- see that class for details.
 */
class ForeignKeyChildCheckWriteFactory implements LocalWriteFactory{
    private final FKConstraintInfo fkConstraintInfo;
    private final TxnOperationFactory txnOperationFactory;

    ForeignKeyChildCheckWriteFactory(FKConstraintInfo fkConstraintInfo,
                                     TxnOperationFactory txnOperationFactory) {
        this.fkConstraintInfo = fkConstraintInfo;
        this.txnOperationFactory = txnOperationFactory;
    }

    @Override
    public void addTo(PipelineWriteContext ctx, boolean keepState, int expectedWrites) throws IOException {
        ctx.addLast(new ForeignKeyChildCheckWriteHandler(ctx.getTransactionalRegion(),fkConstraintInfo,txnOperationFactory));
    }

    @Override
    public long getConglomerateId() {
        throw new UnsupportedOperationException("not used");
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