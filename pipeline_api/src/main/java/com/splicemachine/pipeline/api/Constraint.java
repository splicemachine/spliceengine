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

package com.splicemachine.pipeline.api;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.ByteSlice;
import java.io.IOException;
import java.util.Set;

/**
 * A Constraint on a Table.
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public interface Constraint {

    /**
     * The type of the Constraint
     */
    public static enum Type {
        /**
         * a PrimaryKey constraint. This is a unique, non-null constraint on one or more columns in the row.
         */
        PRIMARY_KEY,
        /**
         * a Unique constraint on one or more columns in a row
         */
        UNIQUE,
        /**
         * a Unique constraint on one or more columns in a row that allows duplicate null values in those columns
         */
        UNIQUE_WITH_DUPLICATE_NULLS,
        /**
         * a Foreign Key constraint, requiring a mapping between one or more columns in a row and a primary key
         * on a separate table
         */
        FOREIGN_KEY,
        /**
         * a Check Constraint.
         */
        CHECK,
        /**
         * A Not-Null Constraint.
         */
        NOT_NULL,
        /**
         * no constraint
         */
        NONE //used for NoConstraint
    }

    /**
     * The result of the Constraint
     */
    public static enum Result {
        SUCCESS,
        FAILURE,
        ADDITIVE_WRITE_CONFLICT
    }

    /**
     * Not sure why the logic in our BatchConstraintChecker (extends ConstraintChecker) can't be in our
     * Constraint implementations directly.
     */
    BatchConstraintChecker asChecker();

    /**
     * @return the type of constraint
     */
    Type getType();

    /**
     * Validate constraint is satisfied on the given mutation.  This method appears to currently be used only within the
     * context of a given BatchWrite -- used to validate the mutations within that batch do not violate the constraint.
     * BatchConstraintChecker is then used to fully validate the constraint.
     */
    Result validate(Record mutation, Txn txn, ServerControl rce, Set<ByteSlice> priorValues) throws IOException;

    ConstraintContext getConstraintContext();

}

