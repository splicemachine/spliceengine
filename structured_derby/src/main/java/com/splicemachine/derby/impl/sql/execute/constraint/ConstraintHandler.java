package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.regionserver.HRegion;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintHandler implements WriteHandler {
    private final Constraint localConstraint;
    private List<Mutation> mutations = Lists.newArrayList();

    private boolean failed=false;

    public ConstraintHandler(Constraint localConstraint) {
        this.localConstraint = localConstraint;
    }

    @Override
    public void next(Mutation mutation, WriteContext ctx) {
//        if(!HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(),mutation.getRow())){
//            ctx.failed(mutation,new MutationResult(MutationResult.Code.FAILED,"WrongRegion"));
//        }else{
//            mutations.add(mutation);
//            ctx.sendUpstream(mutation);
//        }
        if(failed)
            ctx.notRun(mutation);
        try {
            if(!HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(),mutation.getRow())){
                //we can't check the mutation, it'll explode
                ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED,"WrongRegion"));
            }else if(!localConstraint.validate(mutation,ctx.getCoprocessorEnvironment())){
                failed = true;
                ctx.result(mutation,
                        new MutationResult(Constraints.convertType(localConstraint.getType()), localConstraint.getConstraintContext()));
            }else
                ctx.sendUpstream(mutation);
        } catch (Exception e) {
            failed=true;
            ctx.failed(mutation, new MutationResult(MutationResult.Code.FAILED, e.getClass().getSimpleName()+":"+e.getMessage()));
        }
    }

    @Override
    public void finishWrites(final WriteContext ctx) throws IOException {
        //no-op
//        boolean failed=false;
//
//        Predicate<Mutation> runPredicate = new Predicate<Mutation>() {
//            @Override
//            public boolean apply(@Nullable Mutation input) {
//                return ctx.canRun(input);
//            }
//        };
//        for(Mutation mutation: Collections2.filter(mutations,runPredicate)) {
//            if(failed)
//                ctx.notRun(mutation);
//
////            if(ctx.getRegion().isClosing()||ctx.getRegion().isClosed()){
////                ctx.failed(mutation,new MutationResult(MutationResult.Code.FAILED,"NotServingRegion"));
////            }
//            try{
//                if(!localConstraint.validate(mutation,ctx.getCoprocessorEnvironment())){
//                    failed=true;
//                    ctx.result(mutation,new MutationResult(Constraints.convertType(localConstraint.getType()),localConstraint.getConstraintContext()));
//                }
//            }catch(IOException ioe){
//                failed=true;
//                ctx.result(mutation,new MutationResult(MutationResult.Code.FAILED,ioe.getClass().getSimpleName()+":"+ioe.getMessage()));
//            }
//        }
//        Collection<Mutation> failedWrites = localConstraint.validate(mutations,ctx.getCoprocessorEnvironment());
//        if(failedWrites.size()>0){
//            MutationResult result = new MutationResult(Constraints.convertType(localConstraint.getType()), localConstraint.getConstraintContext());
//            for(Mutation mutation:failedWrites){
//                ctx.failed(mutation,result);
//            }
//        }
    }
}
