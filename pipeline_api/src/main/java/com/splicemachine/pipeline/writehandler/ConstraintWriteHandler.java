/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.pipeline.writehandler;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintWriteHandler implements WriteHandler {

    private static final WriteResult additiveWriteConflict = WriteResult.failed("Additive WriteConflict");

    private final Constraint localConstraint;
    private boolean failed;
    private Set<ByteSlice> visitedRows;
    private final WriteResult invalidResult;
    private int expectedWrites;
    private final PipelineExceptionFactory exceptionFactory;

    public ConstraintWriteHandler(Constraint localConstraint,int expectedWrites,PipelineExceptionFactory exceptionFactory) {
        this.expectedWrites = expectedWrites;
        this.localConstraint = localConstraint;
        this.exceptionFactory=exceptionFactory;
        this.invalidResult = new WriteResult(WriteResult.convertType(localConstraint.getType()), localConstraint.getConstraintContext());
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (visitedRows == null) {
            int initialCapacity = (int) Math.ceil(2*expectedWrites/0.9f);
//            visitedRows = new TreeSet<>();
            visitedRows = new HashSet<>(initialCapacity,0.9f);
        }
        if (failed) {
            ctx.notRun(mutation);
        }
        if(!containsRow(ctx,mutation.rowKeySlice())){
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
            // We don't need deletes in this set. delete -> insert/upsert to same rowkey is inefficient but valid
            if (mutation.getType() != KVPair.Type.DELETE) {
                visitedRows.add(mutation.rowKeySlice());
            }
        } catch (IOException nsre) {
            failed = true;
            //noinspection ThrowableResultOfMethodCallIgnored
            if(exceptionFactory.processPipelineException(nsre)instanceof NotServingPartitionException){
                ctx.failed(mutation,WriteResult.notServingRegion());
            }else
                ctx.failed(mutation, WriteResult.failed(nsre.getClass().getSimpleName()+":"+nsre.getMessage()));
        } catch (Exception e) {
            failed = true;
            ctx.failed(mutation, WriteResult.failed(e.getClass().getSimpleName() + ":" + e.getMessage()));
        }
    }

    private boolean containsRow(WriteContext ctx,ByteSlice byteSlice) {
        return ctx.getRegion().containsRow(byteSlice.array(),byteSlice.offset(),byteSlice.length());
    }

    @Override
    public void flush(final WriteContext ctx) throws IOException {
        if (visitedRows != null) {
            visitedRows.clear();
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (visitedRows != null) {
            visitedRows.clear();
        }
        visitedRows = null;
    }
}
