package com.splicemachine.pipeline.constraint;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.base.Predicate;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;

/**
 * A Unique Constraint
 *
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {

    private final ConstraintContext constraintContext;

    private static final Predicate<? super KVPair> INSERT_UPSERT_PREDICATE = new Predicate<KVPair>() {
        @Override
        public boolean apply(KVPair input) {
            KVPair.Type type = input.getType();
            return type == KVPair.Type.INSERT || type == KVPair.Type.UPSERT;
        }
    };

    public static Constraint create(ConstraintContext cc) {
        return new UniqueConstraint(cc);
    }

    public UniqueConstraint(ConstraintContext constraintContext) {
        this.constraintContext = constraintContext;
    }

    @Override
    public BatchConstraintChecker asChecker() {
        return new UniqueConstraintChecker(false, constraintContext);
    }

    @Override
    public Type getType() {
        return Type.UNIQUE;
    }

    @Override
    public Result validate(KVPair mutation, TxnView txn, RegionCoprocessorEnvironment rce, ObjectOpenHashSet<KVPair> priorValues) throws IOException {
        if (!INSERT_UPSERT_PREDICATE.apply(mutation)) {
            return Result.SUCCESS; //no need to validate this mutation
        }
        // if prior visited values has it, it's in the same batch mutation, so don't fail it
        if (!priorValues.contains(mutation)) {
            return Result.SUCCESS;
        }
        return (mutation.getType() == KVPair.Type.UPSERT) ? Result.ADDITIVE_WRITE_CONFLICT : Result.FAILURE;
    }

    @Override
    public ConstraintContext getConstraintContext() {
        return constraintContext;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UniqueConstraint)) return false;
        UniqueConstraint that = (UniqueConstraint) o;
        return constraintContext.equals(that.constraintContext);
    }

    @Override
    public int hashCode() {
        return constraintContext.hashCode();
    }

}
