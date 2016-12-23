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

package com.splicemachine.si.api.server;

import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Record;
import java.io.IOException;

/**
 * Hook for external systems to apply constraint checking based on rows returned during conflict detection.
 *
 * This serves multiple purposes:
 *
 * 1. Efficiency: A row only needs to be fetched a single time, and is then passed through Conflict detection
 * AND constraint checks. This minimizes the amount of IO needed to perform a constraint check.
 * 2. Correctness: Due to the concurrency of the write pipeline, some constraints are only correct if they are
 * applied synchronously (that is, within the HBase row lock). This mechanism guarantees that there is a mechanism
 * to properly apply that constraint.
 *
 * @author Scott Fines
 *         Date: 3/14/14
 */
public interface ConstraintChecker{

    /**
     * Checks the constraint against the current row and the current modification.
     *
     * The {@code existingRow} entity is guaranteed to be a row which exists (e.g. there are KeyValues there) and
     * is visible to the transaction that this constraint is to be applied within.
     *
     * For example, suppose you are checking a constraint on row A with transaction t.
     *
     * If row A does not exist yet in HBase, this method will not be called and the constraint will not be checked.
     *
     * If row A already exists, but was written with a conflicting transaction, this method will not be checked
     * (because a Write/Write conflict will be thrown).
     *
     * If row A exists, but the row is not visible to the current transaction (i.e. it was rolled back), then
     * this method will not be checked.
     *
     * If row A exists, and the row is visible to transaction t, this method will be called.
     *
     * @param mutation    the attempted write row
     * @param existingRow the row which exists, and which is visible to the write transaction
     * @return a Status entity representing the Constraint's conclusion
     * @throws IOException
     */
    MutationStatus checkConstraint(Record mutation, Record existingRow) throws IOException;
}
