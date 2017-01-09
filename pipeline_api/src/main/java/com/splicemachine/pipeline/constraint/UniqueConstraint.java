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

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import com.splicemachine.utils.ByteSlice;
import java.io.IOException;
import java.util.Set;

/**
 * A Unique Constraint.
 *
 * Used as part of the bulk write pipeline this class just verifies uniqueness among the newly written values only.
 * Uniqueness relative to the existing persistent values is handled by UniqueConstraintChecker and related classes.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {

    private final ConstraintContext constraintContext;
    protected final OperationStatusFactory opStatusFactory;

    public UniqueConstraint(ConstraintContext constraintContext,OperationStatusFactory opStatusFactory) {
        this.constraintContext = constraintContext;
        this.opStatusFactory = opStatusFactory;
    }

    @Override
    public BatchConstraintChecker asChecker() {
        return new UniqueConstraintChecker(false, constraintContext,opStatusFactory);
    }

    @Override
    public Type getType() {
        return Type.UNIQUE;
    }

    @Override
    public Result validate(Record mutation, Txn txn, ServerControl rce, Set<ByteSlice> priorValues) throws IOException {
        RecordType type = mutation.getRecordType();
        // Only these mutation types can cause UniqueConstraint violations.
        if (type == RecordType.INSERT || type == RecordType.UPSERT) {
            // if prior visited values has it, it's in the same batch mutation, so don't fail it
            if (priorValues.contains(mutation.rowKeySlice())) {
                return (type == RecordType.UPSERT) ? Result.ADDITIVE_WRITE_CONFLICT : Result.FAILURE;
            }
        }
        return Result.SUCCESS;
    }

    @Override
    public ConstraintContext getConstraintContext() {
        return constraintContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UniqueConstraint)) return false;
        UniqueConstraint that = (UniqueConstraint) o;
        return constraintContext.equals(that.constraintContext);
    }

    @Override
    public int hashCode() {
        return constraintContext.hashCode();
    }

    @Override
    public String toString() {
        return "{" + constraintContext + "}";
    }
}
