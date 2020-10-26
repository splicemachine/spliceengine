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

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.foreignkey.ForeignKeyViolationProcessor;
import com.splicemachine.si.api.data.TxnOperationFactory;

public class OnDeleteNoAction extends OnDeleteAbstractAction {

    private final String parentTableName;

    public OnDeleteNoAction(Long backingIndexConglomId,
                            DDLMessage.FKConstraintInfo constraintInfo,
                            WriteContext writeContext,
                            String parentTableName,
                            TxnOperationFactory txnOperationFactory,
                            ForeignKeyViolationProcessor violationProcessor) throws Exception {
        super(backingIndexConglomId, constraintInfo, writeContext, txnOperationFactory, violationProcessor);
        this.parentTableName = parentTableName;
    }

    @Override
    protected WriteResult handleExistingRow(byte[] indexRowId, KVPair sourceMutation) {
        ConstraintContext context = ConstraintContext.foreignKey(constraintInfo, sourceMutation.getRowKey());
        failed = true;
        return new WriteResult(Code.FOREIGN_KEY_VIOLATION, context.withMessage(1, parentTableName));
    }
}
