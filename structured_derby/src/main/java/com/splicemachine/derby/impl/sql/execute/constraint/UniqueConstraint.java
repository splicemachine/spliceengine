package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * A Unique Constraint
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class UniqueConstraint implements Constraint {
    private static final Logger logger = Logger.getLogger(UniqueConstraint.class);
    private static final Predicate<? super Mutation> stripDeletes = new Predicate<Mutation>() {
        @Override
        public boolean apply(@Nullable Mutation input) {
            if (!(input instanceof Put)) return false;
            return !Arrays.equals(input.getAttribute(Puts.PUT_TYPE), Puts.FOR_UPDATE);
        }
    };
    private static final Function<? super Mutation, Get> validator = new Function<Mutation, Get>() {
        @Override
        public Get apply(@Nullable Mutation input) {
            try {
                Get get = SpliceUtils.createGet(input, input.getRow());
                get.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);
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
        if(!HRegion.rowIsInRange(region.getRegionInfo(),get.getRow())){

        }
        Result result = region.get(get,null);

        boolean rowPresent = result!=null && !result.isEmpty();
        SpliceLogUtils.trace(logger,rowPresent? "row exists!": "row not yet present");
        if(rowPresent)
            SpliceLogUtils.trace(logger,result.toString());
        return !rowPresent;
    }

    @Override
    public boolean validate(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException {
        Collection<Get> putsToValidate = Collections2.transform(Collections2.filter(mutations,stripDeletes),validator);

        HRegion region = rce.getRegion();
        for(Get get:putsToValidate){
            Result result = region.get(get,null);
            boolean rowPresent =result!=null && ! result.isEmpty();
            if(rowPresent) return false;
        }
        return true;
    }

    public static Constraint create() {
        return new UniqueConstraint();
    }

}
