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

import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Record;

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
    public MutationStatus checkConstraint(Record mutation, Record existingRow) throws IOException {
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
