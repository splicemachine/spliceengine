package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
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
						return input.getType()==KVPair.Type.INSERT;
        }
    };

    private Get createGet(KVPair kvPair,String txnId) throws IOException {
				byte[] txnIdBytes = Bytes.toBytes(txnId);
				Get get = new Get(kvPair.getRow());
				get.setAttribute(SIConstants.SI_NEEDED,SIConstants.TRUE_BYTES);
				get.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,txnIdBytes);
        return get;
    }

    public Type getType(){
        return Type.UNIQUE;
    }

    @Override
    public boolean validate(KVPair mutation,String txnId, RegionCoprocessorEnvironment rce,Collection<KVPair> priorValues) throws IOException {
        if(!stripDeletes.apply(mutation)) return true; //no need to validate this mutation
        //if prior visited values has it, it's in the same batch mutation, so fail it
        if(priorValues.contains(mutation)){
						if (logger.isTraceEnabled())
								SpliceLogUtils.trace(logger, "row %s",BytesUtil.toHex(mutation.getRow()));
            return false;
				}
        Get get = createGet(mutation,txnId);

        HRegion region = rce.getRegion();
        //check the Bloom Filter first--if it's not present, then we know we're good
        if (!HRegionUtil.keyExists(region.getStore(SIConstants.DEFAULT_FAMILY_BYTES), mutation.getRow()))
        		return true;

        Result result = region.get(get);
        boolean rowPresent = result!=null && !result.isEmpty();
        if(rowPresent){
            KeyValue[] raw = result.raw();
            rowPresent=false;
            for(KeyValue kv:raw){
                    rowPresent=true;
                    if (logger.isTraceEnabled())
                    	SpliceLogUtils.trace(logger, "row %s,CF %s present",BytesUtil.toHex(mutation.getRow()),BytesUtil.toHex(kv.getFamily()));
                    break;
            }
        }
        return !rowPresent;
    }

    @Override
    public List<KVPair> validate(Collection<KVPair> mutations, String txnId,RegionCoprocessorEnvironment rce,List<KVPair> priorValues) throws IOException {
        Collection<KVPair> changes = Collections2.filter(mutations,stripDeletes);
        List<KVPair> failedKvs = Lists.newArrayListWithExpectedSize(0);
        for(KVPair change:changes){
            if(HRegionUtil.keyExists(rce.getRegion().getStore(SIConstants.DEFAULT_FAMILY_BYTES), change.getRow()) && !validate(change,txnId,rce,priorValues))
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
