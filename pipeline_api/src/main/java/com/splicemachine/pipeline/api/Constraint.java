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

package com.splicemachine.pipeline.api;

import com.splicemachine.access.api.ServerControl;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.constraint.BatchConstraintChecker;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import java.io.IOException;
import java.util.Map;

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
    enum Type {
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
    enum Result {
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
    Result validate(KVPair mutation, TxnView txn, ServerControl rce, Map<ByteSlice, ByteSlice> priorValues) throws IOException;

    ConstraintContext getConstraintContext();

}

