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
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;

public class NoOpConstraintChecker implements BatchConstraintChecker {
    private final MutationStatus SUCCESS;

    public NoOpConstraintChecker(OperationStatusFactory statusLib) {
        this.SUCCESS = statusLib.success();
    }

    @Override
    public MutationStatus checkConstraint(KVPair mutation, DataResult existingRow) throws IOException {
        return SUCCESS;
    }

    @Override
    public WriteResult asWriteResult(MutationStatus opStatus) {
        throw new NotImplementedException();
    }

    @Override
    public boolean matches(MutationStatus status) {
        return false;
    }
}
