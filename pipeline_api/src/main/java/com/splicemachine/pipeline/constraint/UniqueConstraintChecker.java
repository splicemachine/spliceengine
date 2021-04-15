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

package com.splicemachine.pipeline.constraint;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public class UniqueConstraintChecker implements BatchConstraintChecker {
    private final MutationStatus SUCCESS;

    private final WriteResult result;
    private final boolean isPrimaryKey;

    private MutationStatus failure;


    public UniqueConstraintChecker(boolean isPrimaryKey,
                                   ConstraintContext constraintContext,
                                   OperationStatusFactory statusLib) {
        this.isPrimaryKey = isPrimaryKey;
        this.result = new WriteResult(isPrimaryKey ? Code.PRIMARY_KEY_VIOLATION : Code.UNIQUE_VIOLATION, constraintContext);
        this.SUCCESS = statusLib.success();
        this.failure = statusLib.failure(isPrimaryKey? "PrimaryKey": "UniqueConstraint");
    }

    @Override
    public MutationStatus checkConstraint(KVPair mutation, DataResult existingRow) throws IOException {

        if (isPrimaryKey) {
            // There is an existing row for the primary key columns, if this is an insert then fail.
            return mutation.getType() == KVPair.Type.INSERT ? failure : SUCCESS;
        }else {
            byte[] existingValue = existingRow.userData().value();
            byte[] value = mutation.getValue();
            boolean valueEqual = Bytes.equals(value, existingValue);
            // If we have seen this index row before, and they are indexing the same main table row, the index row
            // is being retried by client. Otherwise it is a true conflict.
            if (!valueEqual) {
                return mutation.getType() == KVPair.Type.INSERT ? failure : SUCCESS;
            } else {
                return SUCCESS;
            }
        }
    }

    @Override
    public WriteResult asWriteResult(MutationStatus opStatus) {
        return result;
    }

    @Override
    public boolean matches(MutationStatus status) {
        if (isPrimaryKey)
            return "PrimaryKey".equals(status.errorMessage());
        else
            return "UniqueConstraint".equals(status.errorMessage());
    }
}
