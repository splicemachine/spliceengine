package com.splicemachine.pipeline.writehandler;

import java.io.IOException;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.log4j.Logger;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteFailedException;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public abstract class AbstractIndexWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(AbstractIndexWriteHandler.class);
    private final byte[] indexConglomBytes;
    private boolean failed = false;
    protected final ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap = ObjectObjectOpenHashMap.newInstance();

    /*
     * The columns in the main table which are indexed (ordered by the position in the main table).
     */
    protected final BitSet indexedColumns;

    protected final BitSet descColumns;
    protected final boolean keepState;

    protected AbstractIndexWriteHandler(BitSet indexedColumns, byte[] indexConglomBytes, BitSet descColumns, boolean keepState) {
        this.indexedColumns = indexedColumns;
        this.indexConglomBytes = indexConglomBytes;
        this.descColumns = descColumns;
        this.keepState = keepState;
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "next %s", mutation);
        if (failed) // Do not run...
            ctx.notRun(mutation);
        else {
            if (!isHandledMutationType(mutation.getType())) // Send Upstream
                ctx.sendUpstream(mutation);
            if (updateIndex(mutation, ctx))
                ctx.sendUpstream(mutation);
            else
                failed = true;
        }
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flush, calling subflush");
        try {
            subFlush(ctx);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            if (e instanceof WriteFailedException) {
                WriteFailedException wfe = (WriteFailedException) e;
                Object[] buffer = indexToMainMutationMap.values;
                int size = indexToMainMutationMap.size();
                for (int i = 0; i < size; i++) {
                    ctx.failed((KVPair) buffer[i], WriteResult.failed(wfe.getMessage()));
                }
            } else throw new IOException(e); //something unexpected went bad, need to propagate
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close, calling subClose");
        try {
            subClose(ctx);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            if (e instanceof WriteFailedException) {
                WriteFailedException wfe = (WriteFailedException) e;
                Object[] buffer = indexToMainMutationMap.values;
                int size = indexToMainMutationMap.size();
                for (int i = 0; i < size; i++) {
                    ctx.failed((KVPair) buffer[i], WriteResult.failed(wfe.getMessage()));
                }
            } else throw new IOException(e); //something unexpected went bad, need to propagate
        }
    }

    protected abstract boolean isHandledMutationType(KVPair.Type type);

    protected abstract boolean updateIndex(KVPair mutation, WriteContext ctx);

    protected abstract void subFlush(WriteContext ctx) throws Exception;

    protected abstract void subClose(WriteContext ctx) throws Exception;

    protected CallBuffer<KVPair> getWriteBuffer(final WriteContext ctx, int expectedSize) throws Exception {
        return ctx.getSharedWriteBuffer(indexConglomBytes, indexToMainMutationMap, expectedSize * 2 + 10, true, ctx.getTxn()); //make sure we don't flush before we can
    }
}