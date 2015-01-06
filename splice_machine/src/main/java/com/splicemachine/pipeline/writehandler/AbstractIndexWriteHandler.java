package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteFailedException;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public abstract class AbstractIndexWriteHandler implements WriteHandler {
    private static final Logger LOG = Logger.getLogger(AbstractIndexWriteHandler.class);
    protected final byte[] indexConglomBytes;
    protected boolean failed;
    protected final ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap = ObjectObjectOpenHashMap.newInstance();

    /*
     * The columns in the main table which are indexed (ordered by the position in the main table).
     */
    protected final BitSet indexedColumns;

    /*
     * The columns in the index table (e.g. mainColToIndexPosMap[indexedColumns.get()])
     */
    protected final BitSet translatedIndexColumns;
    /*
     * Mapping between the position in the main column's data stream, and the position in the index
     * key. The length of this is the same as the number of columns in the main table, and if the
     * fields isn't in the index, then the value of this map should be -1.
     */
    protected final int[] mainColToIndexPosMap;

    protected final BitSet descColumns;
    protected final boolean keepState;

    protected AbstractIndexWriteHandler(BitSet indexedColumns, int[] mainColToIndexPosMap, byte[] indexConglomBytes, BitSet descColumns, boolean keepState) {
        this.indexedColumns = indexedColumns;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexConglomBytes = indexConglomBytes;
        this.descColumns = descColumns;
        this.keepState = keepState;

        this.translatedIndexColumns = new BitSet(indexedColumns.cardinality());
        for (int i = indexedColumns.nextSetBit(0); i >= 0; i = indexedColumns.nextSetBit(i + 1)) {
            translatedIndexColumns.set(mainColToIndexPosMap[i]);
        }
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "next %s", mutation);
        if (failed)
            ctx.notRun(mutation);
        else {
            boolean sendUp = updateIndex(mutation, ctx);
            if (sendUp) {
                ctx.sendUpstream(mutation);
            }
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

    protected abstract boolean updateIndex(KVPair mutation, WriteContext ctx);

    protected abstract void subFlush(WriteContext ctx) throws Exception;

    protected abstract void subClose(WriteContext ctx) throws Exception;

    protected CallBuffer<KVPair> getWriteBuffer(final WriteContext ctx, int expectedSize) throws Exception {
        return ctx.getSharedWriteBuffer(indexConglomBytes, indexToMainMutationMap, expectedSize * 2 + 10, true, ctx.getTxn()); //make sure we don't flush before we can
    }

    protected void accumulate(EntryAccumulator newKeyAccumulator, BitIndex updateIndex, int newPos, byte[] data, int offset, int length) {
        if (updateIndex.isScalarType(newPos))
            newKeyAccumulator.addScalar(mainColToIndexPosMap[newPos], data, offset, length);
        else if (updateIndex.isFloatType(newPos))
            newKeyAccumulator.addFloat(mainColToIndexPosMap[newPos], data, offset, length);
        else if (updateIndex.isDoubleType(newPos))
            newKeyAccumulator.addDouble(mainColToIndexPosMap[newPos], data, offset, length);
        else
            newKeyAccumulator.add(mainColToIndexPosMap[newPos], data, offset, length);
    }

}