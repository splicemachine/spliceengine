package com.splicemachine.pipeline.constraint;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

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

//    @Override
    public List<KVPair> validate(Collection<KVPair> mutations, TxnView txn,RegionCoprocessorEnvironment rce,ObjectOpenHashSet<KVPair> priorValues) throws IOException {
        Collection<KVPair> changes = Collections2.filter(mutations, INSERT_UPSERT_PREDICATE);
        List<KVPair> failedKvs = Lists.newArrayListWithExpectedSize(0);
        for (KVPair change : changes) {
            if (HRegionUtil.keyExists(rce.getRegion().getStore(SIConstants.DEFAULT_FAMILY_BYTES), change.getRowKey()) && validate(change, txn, rce, priorValues) != Constraint.Result.SUCCESS)
                failedKvs.add(change);
        }
        return failedKvs;
    }

    @Override
    public Result validate(KVPair mutation, TxnView txn, RegionCoprocessorEnvironment rce, ObjectOpenHashSet<KVPair> priorValues) throws IOException {
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
