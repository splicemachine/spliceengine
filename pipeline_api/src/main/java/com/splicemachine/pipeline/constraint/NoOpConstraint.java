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

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

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
public class NoOpConstraint implements Constraint {

    protected final OperationStatusFactory opStatusFactory;

    public NoOpConstraint(OperationStatusFactory opStatusFactory) {
        this.opStatusFactory = opStatusFactory;
    }

    @Override
    public BatchConstraintChecker asChecker() {
        return new NoOpConstraintChecker(opStatusFactory);
    }

    @Override
    public Type getType() {
        return Type.NO_OP;
    }

    @Override
    public Result validate(KVPair mutation, TxnView txn, ServerControl rce, Map<ByteSlice, ByteSlice> priorValues) throws IOException {
        return Result.SUCCESS;
    }

    @Override
    public ConstraintContext getConstraintContext() {
        throw new NotImplementedException();
    }
}
