package com.splicemachine.pipeline.foreignkey.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.ForeignKeyViolationProcessor;
import com.splicemachine.si.api.data.TxnOperationFactory;

public class ActionFactory {
    public static Action createAction(Long childBaseTableConglomId,
                                      Long backingIndexConglomId,
                                      DDLMessage.FKConstraintInfo constraintInfo,
                                      WriteContext writeContext,
                                      String parentTableName,
                                      TxnOperationFactory txnOperationFactory,
                                      ForeignKeyViolationProcessor violationProcessor) throws Exception {
        switch(constraintInfo.getDeleteRule()) {
            case StatementType.RA_SETNULL:
                return new OnDeleteSetNull(childBaseTableConglomId,
                        backingIndexConglomId, constraintInfo, writeContext, txnOperationFactory, violationProcessor);
            case StatementType.RA_NOACTION:
                return new OnDeleteNoAction(childBaseTableConglomId,
                        backingIndexConglomId, constraintInfo, writeContext, parentTableName, txnOperationFactory, violationProcessor);
            default:
                throw StandardException.newException(String.format("Unexpected FK action type %d", constraintInfo.getDeleteRule()));
        }
    }
}
