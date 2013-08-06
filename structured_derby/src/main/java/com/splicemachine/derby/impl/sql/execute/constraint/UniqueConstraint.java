package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.Transactor;
import com.splicemachine.si.coprocessors.RollForwardQueueMap;
import com.splicemachine.si.coprocessors.SIFilterPacked;
import com.splicemachine.si.impl.RollForwardQueue;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.*;

/**
 * A Unique Constraint
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {
    private static final Logger logger = Logger.getLogger(UniqueConstraint.class);

    private final ConstraintContext constraintContext;
    private static final Logger LOG = Logger.getLogger(UniqueConstraint.class);

    public UniqueConstraint(ConstraintContext constraintContext){
        this.constraintContext = constraintContext;
    }

    private static final Predicate<? super Mutation> stripDeletes = new Predicate<Mutation>() {
        @Override
        public boolean apply(@Nullable Mutation input) {
            if(Mutations.isDelete(input)) {
                return false;
            }
            byte[] putType = input.getAttribute(Puts.PUT_TYPE);
            return putType == null || !Bytes.equals(putType, Puts.FOR_UPDATE);
//            return !Bytes.equals(input.getAttribute(Puts.PUT_TYPE), Puts.FOR_UPDATE);
        }
    };

    private static final Function<? super Mutation, Get> validator = new Function<Mutation, Get>() {
        @Override
        public Get apply(@Nullable Mutation input) {
            try {
                Get get = SpliceUtils.createGet(input, input.getRow());
                get.addFamily(SpliceConstants.DEFAULT_FAMILY_BYTES);
                EntryPredicateFilter predicateFilter = EntryPredicateFilter.emptyPredicate();
                get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
                return get;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    };

    public Type getType(){
        return Type.UNIQUE;
    }

    @Override
    public boolean validate(Mutation mutation, RegionCoprocessorEnvironment rce) throws IOException {
        if(!stripDeletes.apply(mutation)) return true; //no need to validate this mutation
        Get get = validator.apply(mutation);

        HRegion region = rce.getRegion();
        Result result = region.get(get);
        boolean rowPresent = result!=null && !result.isEmpty();
//        SpliceLogUtils.trace(logger,rowPresent? "row exists!": "row not yet present");
        if(rowPresent){
//            SpliceLogUtils.trace(logger, BytesUtil.toHex(mutation.getRow()));
            KeyValue[] raw = result.raw();
            rowPresent=false;
            for(KeyValue kv:raw){
                if(Bytes.equals(kv.getFamily(),SpliceConstants.DEFAULT_FAMILY_BYTES)&&Bytes.equals(mutation.getRow(),kv.getRow())){
                    rowPresent=true;
                    SpliceLogUtils.trace(logger, "row %s,CF %s present",BytesUtil.toHex(mutation.getRow()),BytesUtil.toHex(kv.getFamily()));
                    break;
                }
            }
        }
        return !rowPresent;
    }

    @Override
    public List<Mutation> validate(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException {
        Collection<Mutation> changes = Collections2.filter(mutations,stripDeletes);
        List<byte[]> rowKeys = Lists.newArrayList(Collections2.transform(changes,new Function<Mutation, byte[]>() {
            @Override
            public byte[] apply(@Nullable Mutation input) {
                return input.getRow();
            }
        }));
        Collections.sort(rowKeys,Bytes.BYTES_COMPARATOR);
        Scan scan = new Scan();
        scan.setStartRow(rowKeys.remove(0));
        scan.setStopRow(BytesUtil.unsignedCopyAndIncrement(rowKeys.get(rowKeys.size()-1)));
        scan.setCaching(1);
        scan.addFamily(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES); //only scan the SI CF for efficiency

        String txnId = null;
        for(Mutation mutation:mutations){
            txnId = SpliceUtils.getTransactionId(mutation);
            break;
        }
        SpliceUtils.attachTransaction(scan,txnId,false);

        HRegion region = rce.getRegion();
        RegionScanner regionScanner = region.getCoprocessorHost().preScannerOpen(scan);
        if(regionScanner==null){
            regionScanner = region.getScanner(scan);
        }
        List<KeyValue> next = Lists.newArrayListWithCapacity(0);
        boolean shouldContinue;
        List<Mutation> failedMutations = Lists.newArrayListWithExpectedSize(0);
        region.startRegionOperation();
        MultiVersionConsistencyControl.setThreadReadPoint(regionScanner.getMvccReadPoint());
        try{
            do{
                shouldContinue = regionScanner.nextRaw(next,null);
                if(!next.isEmpty()){
                    byte[] row = next.get(0).getRow();
                    for(Mutation mutation: mutations){
                        if(Bytes.equals(mutation.getRow(),row)){
                            failedMutations.add(mutation);
                            break;
                        }
                    }
                /*
                 *The row that's returned may not be part of the list, so just seek to the next
                 * entry if that happens
                 */
                    next.clear();
                    if(rowKeys.size()>0)
                        regionScanner.reseek(rowKeys.remove(0));
                    else
                        return failedMutations;
                }
            }while(shouldContinue);
        }finally{
            regionScanner.close();
            region.closeRegionOperation();
        }
        return failedMutations;
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

    private static class UniqueFilter extends FilterBase {
        private List<byte[]> bytesToCheck;
        byte[] next;

        private UniqueFilter(List<byte[]> bytesToCheck) {
            this.bytesToCheck = bytesToCheck;
            //sort in ascending order
            Collections.sort(bytesToCheck,Bytes.BYTES_COMPARATOR);
            next = bytesToCheck.remove(0);
        }

        @Override
        public ReturnCode filterKeyValue(KeyValue ignored) {
            byte[] rowKey = ignored.getRow();
            boolean failed=Bytes.equals(next,rowKey);
            if(!failed){
                for(byte[] bytes:bytesToCheck){
                    if(Bytes.equals(rowKey,bytes)){
                        failed=true;
                        break;
                    }
                }
            }
            if(failed)
                return ReturnCode.INCLUDE;
            else
                return ReturnCode.SEEK_NEXT_USING_HINT;
        }

        @Override
        public KeyValue getNextKeyHint(KeyValue currentKV) {
            return KeyValue.createFirstOnRow(bytesToCheck.remove(0));
        }

        @Override
        public void write(DataOutput out) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            //To change body of implemented methods use File | Settings | File Templates.
        }
    }

    public static void main(String... args) throws Exception{
        System.out.println(Arrays.toString(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES));
        System.out.println(Arrays.toString(SpliceConstants.DEFAULT_FAMILY_BYTES));
    }


}
