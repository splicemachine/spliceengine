/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.foreignkey.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.ForeignKeyViolationProcessor;
import com.splicemachine.si.api.data.TxnOperationFactory;

public class ActionFactory {
    public static Action createAction(Long backingIndexConglomId,
                                      DDLMessage.FKConstraintInfo constraintInfo,
                                      WriteContext writeContext,
                                      String parentTableName,
                                      TxnOperationFactory txnOperationFactory,
                                      ForeignKeyViolationProcessor violationProcessor) throws Exception {
        switch(constraintInfo.getDeleteRule()) {
            case StatementType.RA_SETNULL:
            case StatementType.RA_CASCADE:
                return new OnDeleteSetNullOrCascade(backingIndexConglomId, constraintInfo, writeContext, txnOperationFactory, violationProcessor, parentTableName);
            case StatementType.RA_NOACTION:
                return new OnDeleteNoAction(backingIndexConglomId, constraintInfo, writeContext, parentTableName, txnOperationFactory, violationProcessor);
            default:
                throw StandardException.newException(String.format("Unexpected FK action type %d", constraintInfo.getDeleteRule()));
        }
    }
}
