package com.splicemachine.derby.impl.sql.execute.constraint;

import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import com.splicemachine.si.data.hbase.TransactorFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintHandler implements WriteHandler {
    private final Constraint localConstraint;

    private boolean failed=false;

    public ConstraintHandler(Constraint localConstraint) {
        this.localConstraint = localConstraint;
    }

    @Override
    public void next(Mutation mutation, WriteContext ctx) {
        if(failed)
            ctx.notRun(mutation);
        try {
            if(!localConstraint.validate(mutation,ctx.getCoprocessorEnvironment())){
                failed = true;
                ctx.result(mutation,
                        new MutationResult(Constraints.convertType(localConstraint.getType())));
            }else
                ctx.sendUpstream(mutation);
        } catch (IOException e) {
            ctx.failed(mutation,e.getClass().getSimpleName()+":"+e.getMessage());
        }
    }

    @Override
    public void finishWrites(WriteContext ctx) throws IOException {
        //no-op
    }
}
