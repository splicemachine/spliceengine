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
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.Map;

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
    public Result validate(KVPair mutation, TxnView txn, ServerControl rce, Map<ByteSlice, ByteSlice> priorValues) throws IOException {
        Type constraintType = getType();
        KVPair.Type type = mutation.getType();
        // Only these mutation types can cause UniqueConstraint violations.
        if (type == KVPair.Type.INSERT || type == KVPair.Type.UPSERT) {
            ByteSlice value = priorValues.get(mutation.rowKeySlice());
            if (constraintType == Type.PRIMARY_KEY) {
                if (value != null)
                    return (type == KVPair.Type.UPSERT) ? Result.ADDITIVE_WRITE_CONFLICT : Result.FAILURE;
                else
                    return Result.SUCCESS;
            }
            else {
                if (value != null) {
                    // If we have seen this index row before, and they are indexing the same main table row, the index row
                    // is being retried by client. Otherwise it is a true conflict.
                    if (!value.equals(mutation.valueSlice())) {
                        return (type == KVPair.Type.UPSERT) ? Result.ADDITIVE_WRITE_CONFLICT : Result.FAILURE;
                    }
                }
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
