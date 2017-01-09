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

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;

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
    public MutationStatus checkConstraint(Record mutation, Record existingRow) throws IOException {
        // There is an existing row, if this is an insert then fail.
        return mutation.getRecordType() == RecordType.INSERT ? failure : SUCCESS;
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
