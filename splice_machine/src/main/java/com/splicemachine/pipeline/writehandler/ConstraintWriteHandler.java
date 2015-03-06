package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.ObjectOpenHashSet;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.constraint.Constraint;
import com.splicemachine.pipeline.impl.WriteResult;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintWriteHandler implements WriteHandler {

    private static final WriteResult additiveWriteConflict = WriteResult.failed("Additive WriteConflict");

    private final Constraint localConstraint;
    private boolean failed;
    private ObjectOpenHashSet<KVPair> visitedRows;
    private final WriteResult invalidResult;
    private int expectedWrites;

    public ConstraintWriteHandler(Constraint localConstraint, int expectedWrites) {
    	this.expectedWrites = expectedWrites;
    	this.localConstraint = localConstraint;
        this.invalidResult = new WriteResult(WriteResult.convertType(localConstraint.getType()), localConstraint.getConstraintContext());
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (visitedRows == null) {        	
        	int initialCapacity = (int) Math.ceil(expectedWrites/0.9f);
            visitedRows = new ObjectOpenHashSet<>(initialCapacity,0.9f);
        }
        if (failed) {
            ctx.notRun(mutation);
        }
        if (!HRegion.rowIsInRange(ctx.getRegion().getRegionInfo(), mutation.getRowKey())) {
            //we can't check the mutation, it'll explode
            ctx.failed(mutation, WriteResult.wrongRegion());
            return;
        }
        try {
            Constraint.Result validate = localConstraint.validate(mutation, ctx.getTxn(), ctx.getCoprocessorEnvironment(), visitedRows);
            switch (validate) {
                case FAILURE:
                    ctx.result(mutation, invalidResult);
                    break;
                case ADDITIVE_WRITE_CONFLICT:
                    ctx.result(mutation, additiveWriteConflict);
                    break;
                default:
                    ctx.sendUpstream(mutation);
            }
            visitedRows.add(mutation);
        } catch (NotServingRegionException nsre) {
            ctx.failed(mutation, WriteResult.notServingRegion());
            failed = true;
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
    }

    @Override
    public void flush(final WriteContext ctx) throws IOException {
        if (visitedRows != null) {
            visitedRows.clear();
        }
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new RuntimeException("Not Supported");
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (visitedRows != null) {
            visitedRows.clear();
        }
        visitedRows = null;
    }
}
