package com.splicemachine.derby.impl.sql.execute.constraint;

import com.splicemachine.hbase.MutationResult;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.Collection;

/**
 * Utilities relating to Constraints
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class Constraints {
    private Constraints(){} //can't make me!
    /*
     * an Empty Constraint which doesn't check anything. A Nice default
     */
    private static final Constraint EMPTY_CONSTRAINT = new Constraint() {
        @Override
        public Type getType() {
            return Type.NONE;
        }

        @Override
        public boolean validate(Mutation mutation, RegionCoprocessorEnvironment rce) throws IOException {
            return true;
        }

        @Override
        public boolean validate(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException {
            return true;
        }
    };


    /**
     * @return an empty constraint which passes everything
     */
    public static Constraint noConstraint(){
        return EMPTY_CONSTRAINT;
    }

    public static MutationResult.Code convertType(Constraint.Type error) {
        switch (error) {
            case PRIMARY_KEY:
                return MutationResult.Code.PRIMARY_KEY_VIOLATION;
            case UNIQUE:
                return MutationResult.Code.UNIQUE_VIOLATION;
            case FOREIGN_KEY:
                return MutationResult.Code.FOREIGN_KEY_VIOLATION;
            case CHECK:
                return MutationResult.Code.CHECK_VIOLATION;
            default:
                return MutationResult.Code.SUCCESS;
        }
    }

    public static Exception constraintViolation(MutationResult.Code writeErrorCode) {
        switch (writeErrorCode) {
            case PRIMARY_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.PRIMARY_KEY);
            case UNIQUE_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.UNIQUE);
            case FOREIGN_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.FOREIGN_KEY);
            case CHECK_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.CHECK);
        }
        return null;
    }
}
