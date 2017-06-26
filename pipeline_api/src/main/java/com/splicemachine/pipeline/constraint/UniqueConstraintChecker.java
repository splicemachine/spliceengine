/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
