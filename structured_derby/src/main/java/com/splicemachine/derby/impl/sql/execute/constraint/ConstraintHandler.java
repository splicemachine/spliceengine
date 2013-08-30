package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.hbase.writer.WriteResult.Code;
import org.apache.hadoop.hbase.regionserver.HRegion;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintHandler implements WriteHandler {
    private final Constraint localConstraint;
    private List<KVPair> mutations = Lists.newArrayList();

    private boolean failed=false;

    public ConstraintHandler(Constraint localConstraint) {
        this.localConstraint = localConstraint;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if(!HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(),mutation.getRow())){
            ctx.failed(mutation,new WriteResult(Code.FAILED,"WrongRegion"));
        }else{
            mutations.add(mutation);
            ctx.sendUpstream(mutation);
        }
    }

    @Override
    public void finishWrites(final WriteContext ctx) throws IOException {
        //no-op
        boolean failed=false;
        Predicate<KVPair> runPredicate = new Predicate<KVPair>() {
            @Override
            public boolean apply(@Nullable KVPair input) {
                return ctx.canRun(input);
            }
        };
        for(KVPair mutation: Collections2.filter(mutations,runPredicate)) {
            if(failed)
                ctx.notRun(mutation);
            if(ctx.getRegion().isClosing()||ctx.getRegion().isClosed()){
                ctx.failed(mutation,new WriteResult(Code.FAILED,"NotServingRegion"));
            }
        }
        Collection<KVPair> failedWrites = localConstraint.validate(mutations,ctx.getTransactionId(),ctx.getCoprocessorEnvironment());
        if(failedWrites.size()>0){
            WriteResult result = new WriteResult(WriteResult.convertType(localConstraint.getType()), localConstraint.getConstraintContext());
            for(KVPair mutation:failedWrites){
                ctx.failed(mutation,result);
            }
        }
    }
}
