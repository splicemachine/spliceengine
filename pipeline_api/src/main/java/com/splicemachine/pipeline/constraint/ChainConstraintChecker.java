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
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/14/14
 */
public class ChainConstraintChecker implements BatchConstraintChecker {
    private List<BatchConstraintChecker> delegates;

    public ChainConstraintChecker(List<BatchConstraintChecker> delegates) {
        this.delegates = delegates;
    }

    @Override
    public MutationStatus checkConstraint(KVPair mutation, DataResult existingRow) throws IOException {
        MutationStatus status = null;
        for (ConstraintChecker delegate : delegates) {
            status = delegate.checkConstraint(mutation, existingRow);
            if (status != null)
            if (!status.isSuccess())
                return status;
        }
        return status;
    }

    @Override
    public WriteResult asWriteResult(MutationStatus status) {
        for (BatchConstraintChecker checker : delegates) {
            if (checker.matches(status))
                return checker.asWriteResult(status);
        }
        return null;
    }

    @Override
    public boolean matches(MutationStatus status) {
        for (BatchConstraintChecker checker : delegates) {
            if (checker.matches(status))
                return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "ChainConstraintChecker{" +
                "delegates=" + (delegates == null ? 0 : delegates.size()) +
                '}';
    }
}
