/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import java.io.IOException;
import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
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
    protected final ObjectObjectHashMap<KVPair, KVPair> routedToBaseMutationMap= new ObjectObjectHashMap();
    protected final boolean keepState;

    protected RoutingWriteHandler(byte[] destination,boolean keepState) {
        this.destination=destination;
        this.keepState = keepState;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "next %s", mutation);
        if (failed) // Do not run...
            ctx.notRun(mutation);
        else {
            if (!isHandledMutationType(mutation.getType())) { // Send Upstream
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
            for (ObjectCursor<KVPair> pair : routedToBaseMutationMap.values()) {
                fail(pair.value,ctx,e);
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
            for (ObjectCursor<KVPair> pair : routedToBaseMutationMap.values()) {
                fail(pair.value,ctx,e);
            }
        }
    }

    protected abstract boolean isHandledMutationType(KVPair.Type type);

    protected abstract boolean route(KVPair mutation,WriteContext ctx);

    protected abstract void doFlush(WriteContext ctx) throws Exception;

    protected abstract void doClose(WriteContext ctx) throws Exception;

    protected final CallBuffer<KVPair> getRoutedWriteBuffer(final WriteContext ctx,int expectedSize) throws Exception {
        return ctx.getSharedWriteBuffer(destination,routedToBaseMutationMap, expectedSize * 2 + 10, true, ctx.getTxn(), ctx.getToken()); //make sure we don't flush before we can
    }

    protected final void fail(KVPair mutation,WriteContext ctx,Exception e){
        if (LOG.isDebugEnabled()) {
            LOG.debug("fail, ", e);
        }
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = ctx.exceptionFactory().processPipelineException(e);
        if(t instanceof NotServingPartitionException)
            ctx.failed(mutation,WriteResult.notServingRegion());
        else if(t instanceof WrongPartitionException)
            ctx.failed(mutation,WriteResult.wrongRegion());
        else {
            LOG.error("Unexpected exception", t);
            ctx.failed(mutation, WriteResult.failed(t.getClass().getSimpleName() + ":" + t.getMessage()));
        }
    }
}
