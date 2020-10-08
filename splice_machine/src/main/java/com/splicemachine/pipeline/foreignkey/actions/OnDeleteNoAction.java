package com.splicemachine.pipeline.foreignkey.actions;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.ForeignKeyViolationProcessor;
import com.splicemachine.si.api.data.TxnOperationFactory;

public class OnDeleteNoAction extends OnDeleteAbstractAction {

    private final String parentTableName;

    public OnDeleteNoAction(Long childBaseTableConglomId,
                            Long backingIndexConglomId,
                            DDLMessage.FKConstraintInfo constraintInfo,
                            WriteContext writeContext,
                            String parentTableName,
                            TxnOperationFactory txnOperationFactory,
                            ForeignKeyViolationProcessor violationProcessor) throws Exception {
        super(childBaseTableConglomId, backingIndexConglomId, constraintInfo, writeContext, txnOperationFactory, violationProcessor);
        this.parentTableName = parentTableName;
    }

    @Override
    protected WriteResult handleExistingRow(byte[] indexRowId, byte[] sourceRowKey) {
        ConstraintContext context = ConstraintContext.foreignKey(constraintInfo);
        failed = true;
        return new WriteResult(Code.FOREIGN_KEY_VIOLATION, context.withMessage(1, parentTableName));
    }
}
