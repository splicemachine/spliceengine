package com.splicemachine.pipeline.constraint;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

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
    Result validate(KVPair mutation, TxnView txn, RegionCoprocessorEnvironment rce, Set<ByteSlice> priorValues) throws IOException;

    ConstraintContext getConstraintContext();

}

