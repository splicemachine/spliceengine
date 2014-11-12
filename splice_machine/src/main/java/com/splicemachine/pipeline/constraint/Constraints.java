package com.splicemachine.pipeline.constraint;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.exception.ErrorState;
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
        public ValidationType validate(KVPair mutation, TxnView txn,RegionCoprocessorEnvironment rce,ObjectOpenHashSet<KVPair> priorValues) throws IOException {
            return ValidationType.SUCCESS;
        }

        @Override
        public Collection<KVPair> validate(Collection<KVPair> mutations,TxnView txn, RegionCoprocessorEnvironment rce,ObjectOpenHashSet<KVPair> priorValues) throws IOException {
            return Collections.emptyList();
        }

        @Override public ConstraintContext getConstraintContext() { return null; }

				@Override public BatchConstraintChecker asChecker() { return null; }

		};


    /**
     * @return an empty constraint which passes everything
     */
    public static Constraint noConstraint(){
        return EMPTY_CONSTRAINT;
    }

    public static Exception constraintViolation(Code writeErrorCode, ConstraintContext constraintContext) {
        switch (writeErrorCode) {
            case UNIQUE_VIOLATION:
            case PRIMARY_KEY_VIOLATION:
                return ErrorState.LANG_DUPLICATE_KEY_CONSTRAINT.newException(constraintContext.getConstraintName(),constraintContext.getTableName());
            case FOREIGN_KEY_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.FOREIGN_KEY, constraintContext);
            case CHECK_VIOLATION:
                return ConstraintViolation.create(Constraint.Type.CHECK, constraintContext);
        }
        return null;
    }
}
