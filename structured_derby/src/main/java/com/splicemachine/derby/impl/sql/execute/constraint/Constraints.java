package com.splicemachine.derby.impl.sql.execute.constraint;

import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.writer.WriteResult;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

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
        public Collection<Mutation> validate(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException {
            return Collections.emptyList();
        }

        @Override
        public ConstraintContext getConstraintContext() {
            return null;
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

    public static Exception constraintViolation(MutationResult.Code writeErrorCode, ConstraintContext constraintContext) {
        switch (writeErrorCode) {
            case PRIMARY_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.PRIMARY_KEY, constraintContext);
            case UNIQUE_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.UNIQUE, constraintContext);
            case FOREIGN_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, constraintContext);
            case CHECK_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.CHECK, constraintContext);
        }
        return null;
    }

    public static Exception constraintViolation(WriteResult.Code writeErrorCode, ConstraintContext constraintContext) {
        switch (writeErrorCode) {
            case PRIMARY_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.PRIMARY_KEY, constraintContext);
            case UNIQUE_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.UNIQUE, constraintContext);
            case FOREIGN_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, constraintContext);
            case CHECK_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.CHECK, constraintContext);
        }
        return null;
    }
}
