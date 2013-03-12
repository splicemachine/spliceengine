package com.splicemachine.derby.impl.sql.execute.constraint;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.Collection;

/**
 * A Constraint on a Table.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public interface Constraint {

    /**
     * The type of the Constraint
     */
    public static enum Type{
        /**
         * a PrimaryKey constraint. This is a unique, non-null constraint on one or more columns in the row.
         */
        PRIMARY_KEY,
        /**
         * a Unique constraint on one or more columns in a row
         */
        UNIQUE,
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
         * no constraint
         */
        NONE //used for NoConstraint
    }

    /**
     * @return the type of constraint
     */
    Type getType();

    /**
     * Validate that the constraint is satisfied on the given mutation. This may
     * perform IO if need be to obtain other results.
     *
     * @param mutation the mutation to validate
     * @param rce the environment for the mutation
     * @return true if the mutation passes the constraint, false otherwise.
     *
     * @throws IOException if something goes wrong during the validation.
     */
    boolean validate(Mutation mutation,RegionCoprocessorEnvironment rce) throws IOException;

    /**
     * Validate that the constraint is satisfied on all the mutations.
     *
     * @param mutations the mutations to validate
     * @param rce the environment for the mutations
     * @return true if <em>all</em> mutations pass the constraint, false if <em>any</em> constraint fails.
     * @throws IOException if something goes wrong during the validation
     */
    boolean validate(Collection<Mutation> mutations,
                     RegionCoprocessorEnvironment rce) throws IOException;

}

