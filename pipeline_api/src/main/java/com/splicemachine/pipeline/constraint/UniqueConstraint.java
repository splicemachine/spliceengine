/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.txn.TxnView;
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
    public Result validate(KVPair mutation, TxnView txn, ServerControl rce, Set<ByteSlice> priorValues) throws IOException {
        KVPair.Type type = mutation.getType();
        // Only these mutation types can cause UniqueConstraint violations.
        if (type == KVPair.Type.INSERT || type == KVPair.Type.UPSERT) {
            // if prior visited values has it, it's in the same batch mutation, so don't fail it
            if (priorValues.contains(mutation.rowKeySlice())) {
                return (type == KVPair.Type.UPSERT) ? Result.ADDITIVE_WRITE_CONFLICT : Result.FAILURE;
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
