package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.collect.Sets;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.batch.WriteHandler;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.WriteResult;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintHandler implements WriteHandler {
    private final Constraint localConstraint;
    private boolean failed=false;
    private Collection<KVPair> visitedRows;
		private final WriteResult invalidResult;

    public ConstraintHandler(Constraint localConstraint) {
        this.localConstraint = localConstraint;
				 invalidResult = new WriteResult(WriteResult.convertType(localConstraint.getType()), localConstraint.getConstraintContext());
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if(visitedRows==null) visitedRows = Sets.newHashSet();
        if(failed)
            ctx.notRun(mutation);
        if(!HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(),mutation.getRow())){
            //we can't check the mutation, it'll explode
            ctx.failed(mutation,WriteResult.wrongRegion());
						return;
        }
        try {
            if(!localConstraint.validate(mutation,ctx.getTransactionId(),ctx.getCoprocessorEnvironment(),visitedRows)){
								ctx.result(mutation, invalidResult);
            }else{
                ctx.sendUpstream(mutation);
            }
						visitedRows.add(mutation);
        }catch(NotServingRegionException nsre){
            ctx.failed(mutation,WriteResult.notServingRegion());
						failed=true;
        }catch (Exception e) {
            failed=true;
            ctx.failed(mutation,WriteResult.failed(e.getClass().getSimpleName()+":"+e.getMessage()));
        }
    }

    @Override
    public void finishWrites(final WriteContext ctx) throws IOException {
        //no-op
    }

	@Override
	public void next(List<KVPair> mutations, WriteContext ctx) {
		// XXX JLEACH TODO
		throw new RuntimeException("Not Supported");
		
	}
}
