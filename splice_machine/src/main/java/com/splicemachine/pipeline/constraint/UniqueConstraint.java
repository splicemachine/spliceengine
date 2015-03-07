package com.splicemachine.pipeline.constraint;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

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
    public Result validate(KVPair mutation, TxnView txn, RegionCoprocessorEnvironment rce, Set<ByteSlice> priorValues) throws IOException {
        // if prior visited values has it, it's in the same batch mutation, so don't fail it
        if (!priorValues.contains(mutation.rowKeySlice())) {
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
