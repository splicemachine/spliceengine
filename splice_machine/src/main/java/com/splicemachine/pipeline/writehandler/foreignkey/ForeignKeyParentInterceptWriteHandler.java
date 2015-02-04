package com.splicemachine.pipeline.writehandler.foreignkey;

import com.google.common.collect.Maps;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteHandler;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Intercepts deletes from a parent table (primary key or unique index) and sends the rowKey over to the referencing
 * indexes to check for its existence.
 */
public class ForeignKeyParentInterceptWriteHandler implements WriteHandler {

    private final WriteCoordinator writeCoordinator;
    private final List<Long> referencingIndexConglomerateIds;
    private final Map<Long, RecordingCallBuffer<KVPair>> callBufferMap = Maps.newHashMap();
    private final ForeignKeyViolationProcessor violationProcessor;

    public ForeignKeyParentInterceptWriteHandler(String parentTableName, List<Long> referencingIndexConglomerateIds) {
        this.referencingIndexConglomerateIds = referencingIndexConglomerateIds;
        this.writeCoordinator = SpliceDriver.driver().getTableWriter();
        this.violationProcessor = new ForeignKeyViolationProcessor(new ForeignKeyViolationProcessor.ParentFkConstraintContextProvider(parentTableName));
    }

    @Override
    public void next(KVPair mutation, WriteContext ctx) {
        if (isForeignKeyInterceptNecessary(mutation.getType())) {
            KVPair kvPair = new KVPair(mutation.getRowKey(), HConstants.EMPTY_BYTE_ARRAY, KVPair.Type.FOREIGN_KEY_CHILDREN_EXISTENCE_CHECK);
            try {
                for (long indexConglomerateId : this.referencingIndexConglomerateIds) {
                    getTargetCallBuffer(ctx, indexConglomerateId).add(kvPair);
                }
                ctx.success(mutation);
            } catch (Exception e) {
                violationProcessor.failWrite(e, ctx);
            }
        }
        ctx.sendUpstream(mutation);
    }

    private boolean isForeignKeyInterceptNecessary(KVPair.Type type) {
        /* We exist to prevent updates/deletes of rows from the parent table which are referenced by a child.
         * Since we are a WriteHandler on a primary-key or unique-index we can just handle deletes. */
        return type == KVPair.Type.DELETE;
    }

    @Override
    public void next(List<KVPair> mutations, WriteContext ctx) {
        throw new UnsupportedOperationException("never called");
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        try {
            for (CallBuffer<KVPair> callBuffer : callBufferMap.values()) {
                callBuffer.flushBuffer();
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        try {
            for (CallBuffer<KVPair> callBuffer : callBufferMap.values()) {
                callBuffer.close();
            }
        } catch (Exception e) {
            violationProcessor.failWrite(e, ctx);
        }
    }

    /* Only need to create the CallBuffer once, but not until we have a WriteContext */
    private RecordingCallBuffer<KVPair> getTargetCallBuffer(WriteContext ctx, long conglomerateId) {
        if (!callBufferMap.containsKey(conglomerateId)) {
            byte[] tableName = Bytes.toBytes(String.valueOf(conglomerateId));
            RecordingCallBuffer<KVPair> callBuffer = writeCoordinator.writeBuffer(tableName, ctx.getTxn());
            callBufferMap.put(conglomerateId, callBuffer);
        }
        return callBufferMap.get(conglomerateId);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
