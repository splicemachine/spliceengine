/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
import java.util.HashMap;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class ConstraintWriteHandler implements WriteHandler {

    private static final WriteResult additiveWriteConflict = WriteResult.failed("Additive WriteConflict");

    private final Constraint localConstraint;
    private boolean failed;
    private Map<ByteSlice, ByteSlice> visitedRows;
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
            visitedRows = new HashMap<>(initialCapacity,0.9f);
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
                visitedRows.put(mutation.rowKeySlice(), mutation.valueSlice());
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
