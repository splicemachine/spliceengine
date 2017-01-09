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

import java.io.IOException;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import org.apache.log4j.Logger;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * A WriteHandler which routes a KVPair to another conglomerate; either a transformed KVPair(like an index), or
 * the Pair itself(like an AlterTable-type operation).
 *
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public abstract class RoutingWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(RoutingWriteHandler.class);
    private final byte[] destination;
    private boolean failed = false;
    /*
     * A Map from the routed KVPair (i.e. the one that is going from this partition to somewhere else) to the
     * original (i.e. the base partition) KVPair. This allows us to backtrack and identify the source for a given
     * destination write.
     */
    protected final ObjectObjectOpenHashMap<Record, Record> routedToBaseMutationMap= ObjectObjectOpenHashMap.newInstance();
    protected final boolean keepState;

    protected RoutingWriteHandler(byte[] destination,boolean keepState) {
        this.destination=destination;
        this.keepState = keepState;
    }

    @Override
    public void next(Record mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "next %s", mutation);
        if (failed) // Do not run...
            ctx.notRun(mutation);
        else {
            if (!isHandledMutationType(mutation.getRecordType())) { // Send Upstream
                ctx.sendUpstream(mutation);
                return;
            }
            if (route(mutation,ctx))
                ctx.sendUpstream(mutation);
            else
                failed=true;
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flush, calling subflush");
        try {
            doFlush(ctx);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            Object[] buffer = routedToBaseMutationMap.values;
            int size = routedToBaseMutationMap.size();
            for (int i = 0; i < size; i++) {
                fail((Record)buffer[i],ctx,e);
            }
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close, calling subClose");
        try {
            doClose(ctx);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            Object[] buffer = routedToBaseMutationMap.values;
            int size = routedToBaseMutationMap.size();
            for (int i = 0; i < size; i++) {
                fail((Record)buffer[i],ctx,e);
            }
        }
    }

    protected abstract boolean isHandledMutationType(RecordType type);

    protected abstract boolean route(Record mutation,WriteContext ctx);

    protected abstract void doFlush(WriteContext ctx) throws Exception;

    protected abstract void doClose(WriteContext ctx) throws Exception;

    protected final CallBuffer<Record> getRoutedWriteBuffer(final WriteContext ctx,int expectedSize) throws Exception {
        return ctx.getSharedWriteBuffer(destination,routedToBaseMutationMap, expectedSize * 2 + 10, true, ctx.getTxn()); //make sure we don't flush before we can
    }

    protected final void fail(Record mutation,WriteContext ctx,Exception e){
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = ctx.exceptionFactory().processPipelineException(e);
        if(t instanceof NotServingPartitionException)
            ctx.failed(mutation,WriteResult.notServingRegion());
        else if(t instanceof WrongPartitionException)
            ctx.failed(mutation,WriteResult.wrongRegion());
        else
            ctx.failed(mutation, WriteResult.failed(t.getClass().getSimpleName()+":"+t.getMessage()));
    }
}