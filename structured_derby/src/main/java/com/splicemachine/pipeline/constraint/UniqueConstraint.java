package com.splicemachine.pipeline.constraint;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * A Unique Constraint
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {
    private static final Logger logger = Logger.getLogger(UniqueConstraint.class);
		private static byte[] predicateBytes = EntryPredicateFilter.emptyPredicate().toBytes();
		private final ConstraintContext constraintContext;

    public UniqueConstraint(ConstraintContext constraintContext){
        this.constraintContext = constraintContext;
    }

    private static final Predicate<? super KVPair> stripDeletes = new Predicate<KVPair>() {
        @Override
        public boolean apply(@Nullable KVPair input) {
						//noinspection ConstantConditions
            KVPair.Type type = input.getType();
            return type ==KVPair.Type.INSERT||type== KVPair.Type.UPSERT;
        }
    };

    private Get createGet(KVPair kvPair,String txnId) throws IOException {
				byte[] txnIdBytes = Bytes.toBytes(txnId);
				Get get = new Get(kvPair.getRow());
				get.setAttribute(SIConstants.SI_NEEDED,SIConstants.TRUE_BYTES);
				get.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,txnIdBytes);
        return get;
    }

		@Override
		public BatchConstraintChecker asChecker() {
				return new UniqueConstraintChecker(false,constraintContext);
		}

		public Type getType(){
        return Type.UNIQUE;
    }

    @Override
    public ValidationType validate(KVPair mutation,TxnView txn, RegionCoprocessorEnvironment rce,ObjectOpenHashSet<KVPair> priorValues) throws IOException {
        if(!stripDeletes.apply(mutation)) return ValidationType.SUCCESS; //no need to validate this mutation
        //if prior visited values has it, it's in the same batch mutation, so fail it
				if(!priorValues.contains(mutation)) return  ValidationType.SUCCESS;

        if(mutation.getType()== KVPair.Type.UPSERT) return ValidationType.ADDITIVE_WRITE_CONFLICT;
        return ValidationType.FAILURE;
    }

    @Override
    public List<KVPair> validate(Collection<KVPair> mutations, TxnView txn,RegionCoprocessorEnvironment rce,ObjectOpenHashSet<KVPair> priorValues) throws IOException {
        Collection<KVPair> changes = Collections2.filter(mutations,stripDeletes);
        List<KVPair> failedKvs = Lists.newArrayListWithExpectedSize(0);
        for(KVPair change:changes){
            if(HRegionUtil.keyExists(rce.getRegion().getStore(SIConstants.DEFAULT_FAMILY_BYTES), change.getRow()) && validate(change,txn,rce,priorValues)!=ValidationType.SUCCESS)
                failedKvs.add(change);
        }
        return failedKvs;
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

    public static Constraint create(ConstraintContext cc) {
        return new UniqueConstraint(cc);
    }
}
